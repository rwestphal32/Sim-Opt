import streamlit as st
import simpy
import random
import pandas as pd
import plotly.express as px
import numpy as np
import graphviz
import time

st.set_page_config(page_title="DAG Factory Optimizer", layout="wide")

st.title("🏭 Universal DAG Sim-Opt: Kanban & Capacities")
st.markdown("The AI now optimizes in two dimensions: **Machine Quantities** (CAPEX) and **Kanban Buffer Limits** (WIP/Working Capital). Check the boxes to tell the AI which levers it is allowed to pull.")

# --- SIDEBAR: FINANCIALS & SETTINGS ---
with st.sidebar:
    st.header("💸 Financial Economics")
    rev_unit = st.number_input("Revenue per Unit (£)", value=500.0)
    rm_cost = st.number_input("Raw Material Cost/Unit (£)", value=150.0)
    wip_cost = st.number_input("WIP Holding Penalty/Unit/Wk (£)", value=15.0)
    
    st.markdown("---")
    st.header("⏱️ Simulation Settings")
    sim_time = st.slider("Simulation Hours", 8, 168, 40) * 60
    eval_runs = 5   # Lowered to prevent server timeouts during heavy 2D searches
    final_runs = 30 # Rigorous verification runs
    
    run_opt = st.button("🚀 Run AI Sim-Opt Search", type="primary", use_container_width=True)

# --- DYNAMIC TABLES ---
st.subheader("1️⃣ Factory Nodes (Machines, Limits & Constraints)")
default_nodes = pd.DataFrame({
    "Node_ID": ["Raw_Metal", "Raw_Plastic", "Milling", "Molding", "Assembly", "Shipping", "Finished_Goods"],
    "Type": ["Source", "Source", "Machine", "Machine", "Machine", "Machine", "Sink"],
    "Machines_Qty": [1, 1, 2, 3, 2, 1, 1],
    "Max_WIP_Limit": [9999, 9999, 50, 50, 100, 9999, 9999], 
    "Mean_Mins": [5.0, 3.0, 8.0, 6.0, 12.0, 20.0, 0.0],
    "StdDev_Mins": [0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 0.0],
    "Yield_Qty": [1, 1, 1, 1, 1, 50, 50],
    "CAPEX_Base": [0, 0, 150000, 120000, 85000, 40000, 0],
    "OPEX_Weekly": [0, 0, 2000, 2500, 3000, 1500, 0],
    "Optimize_Qty": [False, False, True, False, True, False, False], 
    "Optimize_Limit": [False, False, True, True, True, False, False]
})

edited_nodes = st.data_editor(default_nodes, num_rows="dynamic", use_container_width=True)

st.subheader("2️⃣ Factory Edges (Bill of Materials & Routing)")
default_edges = pd.DataFrame({
    "From_Node": ["Raw_Metal", "Raw_Plastic", "Milling", "Molding", "Assembly", "Shipping"],
    "To_Node": ["Milling", "Molding", "Assembly", "Assembly", "Shipping", "Finished_Goods"],
    "Qty_Required": [1, 1, 1, 2, 50, 50] 
})

edited_edges = st.data_editor(default_edges, num_rows="dynamic", use_container_width=True)

# --- CORE SIMULATION ENGINE ---
def evaluate_network(nodes_df, edges_df, num_runs=5, return_queues=False):
    run_profits, run_tps, run_wips = [], [], []
    sample_q_data = []

    total_capex = sum([row["Machines_Qty"] * row["CAPEX_Base"] for _, row in nodes_df[nodes_df["Type"]=="Machine"].iterrows()])
    total_opex = sum([row["Machines_Qty"] * row["OPEX_Weekly"] for _, row in nodes_df[nodes_df["Type"]=="Machine"].iterrows()])
    weekly_depr = (total_capex / 10.0) / 52.0

    def universal_node_process(env, node_id, node_type, mean_t, std_t, yield_qty, buffers, machines):
        inputs = [(e["From_Node"], int(e["Qty_Required"])) for _, e in edges_df.iterrows() if e["To_Node"] == node_id]

        while True:
            if node_type == "Source":
                yield env.timeout(max(0.1, random.gauss(mean_t, std_t)))
                yield buffers[node_id].put(yield_qty) 
                
            elif node_type == "Machine":
                if inputs:
                    get_events = [buffers[in_node].get(req_qty) for in_node, req_qty in inputs]
                    yield env.all_of(get_events)
                
                with machines[node_id].request() as req:
                    yield req
                    yield env.timeout(max(0.1, random.gauss(mean_t, std_t)))
                
                yield buffers[node_id].put(yield_qty)
                
            elif node_type == "Sink":
                if inputs:
                    get_events = [buffers[in_node].get(req_qty) for in_node, req_qty in inputs]
                    yield env.all_of(get_events)
                    yield buffers[node_id].put(yield_qty)
                    yield env.timeout(0) 
                else:
                    break

    def monitor_network(env, buffers, q_data, save_sample):
        while True:
            total_wip = sum([buf.level for nid, buf in buffers.items() if nodes_df[nodes_df["Node_ID"]==nid]["Type"].values[0] != "Sink"])
            wip_snapshot.append(total_wip)
            if save_sample:
                row = {"Time (Mins)": env.now}
                for nid, buf in buffers.items(): row[f"{nid}_WIP"] = buf.level
                q_data.append(row)
            yield env.timeout(5)

    for r_idx in range(num_runs):
        env = simpy.Environment()
        buffers, machines = {}, {}
        wip_snapshot = []
        save_sample = return_queues and (r_idx == num_runs - 1)
        
        for _, row in nodes_df.iterrows():
            nid = row["Node_ID"]
            b_limit = int(row["Max_WIP_Limit"]) if row["Max_WIP_Limit"] > 0 else 99999
            buffers[nid] = simpy.Container(env, init=0, capacity=b_limit)
            if row["Type"] == "Machine":
                machines[nid] = simpy.Resource(env, capacity=int(max(1, row["Machines_Qty"])))

        for _, row in nodes_df.iterrows():
            env.process(universal_node_process(env, row["Node_ID"], row["Type"], row["Mean_Mins"], row["StdDev_Mins"], row["Yield_Qty"], buffers, machines))
            
        env.process(monitor_network(env, buffers, sample_q_data, save_sample))
        env.run(until=sim_time)
        
        tp_val = sum([buffers[nid].level for nid in nodes_df[nodes_df["Type"]=="Sink"]["Node_ID"]])
        wip_val = np.mean(wip_snapshot) if wip_snapshot else 0
        
        net_profit = (tp_val * rev_unit) - (tp_val * rm_cost) - total_opex - (wip_val * wip_cost) - weekly_depr
        
        run_profits.append(net_profit)
        run_tps.append(tp_val)
        run_wips.append(wip_val)

    res = {"profit": np.mean(run_profits), "tp": np.mean(run_tps), "wip": np.mean(run_wips), "capex": total_capex}
    if return_queues: return res, sample_q_data
    return res

# --- GRAPHVIZ GENERATOR ---
def generate_dag_vsm(nodes_df, edges_df, q_data):
    dot = graphviz.Digraph(node_attr={'shape': 'box', 'style': 'filled', 'color': '#E1E4E8', 'fontname': 'Helvetica'})
    dot.attr(rankdir='LR', splines='ortho')
    df_q = pd.DataFrame(q_data) if q_data else None
    
    for _, row in nodes_df.iterrows():
        nid, ntype, qty = row["Node_ID"], row["Type"], int(row["Machines_Qty"])
        lim = row["Max_WIP_Limit"]
        final_wip = df_q[f"{nid}_WIP"].iloc[-1] if df_q is not None else 0
        
        if ntype == "Source":
            dot.node(f'{nid}_Src', f'{nid}\n(Source)\nμ={row["Mean_Mins"]}m', color='#cce5ff', shape='folder')
            dot.node(f'{nid}_Buf', f'{nid} Buffer\nQty: {final_wip} / {lim}', color='#fff3cd', shape='cylinder')
            dot.edge(f'{nid}_Src', f'{nid}_Buf')
        elif ntype == "Sink":
            dot.node(nid, f'{nid}\n(Sink)\nTotal Finished: {final_wip}', color='#d4edda', shape='folder')
        else:
            buf_color = '#f8d7da' if final_wip >= lim else '#fff3cd'
            dot.node(f'{nid}_Buf', f'{nid} Output\nQty: {final_wip} / {lim}', color=buf_color, shape='cylinder')
            
            with dot.subgraph(name=f'cluster_{nid}') as s:
                s.attr(style='invis')
                for i in range(qty):
                    s.node(f'{nid}_M{i}', f'{nid} {i+1}', color='#e2e3e5')
                    s.edge(f'{nid}_M{i}', f'{nid}_Buf')

    for _, edge in edges_df.iterrows():
        frm, to, req = edge["From_Node"], edge["To_Node"], edge["Qty_Required"]
        to_type = nodes_df[nodes_df["Node_ID"] == to]["Type"].values[0]
        
        if to_type == "Machine":
            qty = int(nodes_df[nodes_df["Node_ID"] == to]["Machines_Qty"].values[0])
            merge_id = f'merge_{frm}_{to}'
            dot.node(merge_id, '', shape='point', width='0')
            dot.edge(f'{frm}_Buf', merge_id, label=f' Pull: {req}', fontcolor='#0056b3', color='#0056b3')
            for i in range(qty): dot.edge(merge_id, f'{to}_M{i}', color='#0056b3')
        elif to_type == "Sink":
            dot.edge(f'{frm}_Buf', to, label=f' Pull: {req}', fontcolor='#2ca02c', color='#2ca02c')
            
    return dot

# --- HILL CLIMBING 2D OPTIMIZER ---
if run_opt:
    with st.spinner(f"Evaluating Client Baseline ({final_runs} Runs)..."):
        base_metrics, base_q_data = evaluate_network(edited_nodes, edited_edges, num_runs=final_runs, return_queues=True)
        
    st_progress = st.empty()
    st_log = st.empty()
    
    current_nodes = edited_nodes.copy()
    best_profit = base_metrics["profit"]
    search_log = ["🔍 **Commencing Cached 2D Financial & Kanban Search...**"]
    
    opt_qty_idx = current_nodes[(current_nodes["Type"] == "Machine") & (current_nodes["Optimize_Qty"] == True)].index.tolist()
    opt_lim_idx = current_nodes[current_nodes["Optimize_Limit"] == True].index.tolist()

    # STATE CACHE (MEMOIZATION)
    visited_states = {}
    def get_hash(df):
        return hash(tuple(df["Machines_Qty"].tolist() + df["Max_WIP_Limit"].tolist()))
    visited_states[get_hash(current_nodes)] = base_metrics

    for step in range(8): 
        st_progress.info(f"Optimization Step {step+1}: Evaluating Kanban & Capacity neighbors...")
        neighbors = []
        
        # Dimension 1: Search Machine Quantities
        for idx in opt_qty_idx:
            if current_nodes.at[idx, "Machines_Qty"] < 10:
                n_df = current_nodes.copy(); n_df.at[idx, "Machines_Qty"] += 1; neighbors.append(n_df)
            if current_nodes.at[idx, "Machines_Qty"] > 1:
                n_df = current_nodes.copy(); n_df.at[idx, "Machines_Qty"] -= 1; neighbors.append(n_df)
                
        # Dimension 2: Search Kanban Limits (Increments of 5)
        for idx in opt_lim_idx:
            lim = current_nodes.at[idx, "Max_WIP_Limit"]
            nid = current_nodes.at[idx, "Node_ID"]
            
            # PREVENT DEADLOCKS: Find largest downstream batch pull. 
            # A Kanban buffer limit can never be smaller than what the downstream machine requires.
            pulls = edited_edges[edited_edges["From_Node"] == nid]["Qty_Required"]
            min_req = int(pulls.max()) if not pulls.empty else 1
            floor_limit = max(5, min_req)
            
            if lim < 500: 
                n_df = current_nodes.copy(); n_df.at[idx, "Max_WIP_Limit"] = lim + 5; neighbors.append(n_df)
            if lim > floor_limit: 
                n_df = current_nodes.copy(); n_df.at[idx, "Max_WIP_Limit"] = max(floor_limit, lim - 5); neighbors.append(n_df)
                
        found_better = False
        for n_df in neighbors:
            s_hash = get_hash(n_df)
            
            # Check Cache before running heavy simulation
            if s_hash in visited_states:
                m = visited_states[s_hash]
            else:
                m = evaluate_network(n_df, edited_edges, num_runs=eval_runs)
                visited_states[s_hash] = m
                
            if m["profit"] > best_profit:
                best_profit = m["profit"]
                current_nodes = n_df
                found_better = True
                search_log.append(f"✅ Improved configuration found. Est. Profit: £{m['profit']:,.0f}/wk")
                st_log.markdown("\n".join(search_log))
                
        # Yield process to webserver to prevent Streamlit WebSocket Timeout
        time.sleep(0.05)
                
        if not found_better:
            search_log.append("🛑 Local Maxima Found. Assets and Limits are financially optimal.")
            st_log.markdown("\n".join(search_log))
            break
            
    st_progress.empty()
    st_log.empty()
    
    with st.spinner(f"Verifying Optimal Configuration ({final_runs} High-Fidelity Runs)..."):
        opt_metrics, opt_q_data = evaluate_network(current_nodes, edited_edges, num_runs=final_runs, return_queues=True)

    # --- UI DASHBOARDS ---
    t1, t2, t3 = st.tabs(["🏆 Gap Analysis", "🗺️ Value Stream Maps", "📈 Queue Physics"])

    with t1:
        st.subheader("Consulting Scorecard: Baseline vs. Optimized")
        d_prof = opt_metrics["profit"] - base_metrics["profit"]
        d_tp = opt_metrics["tp"] - base_metrics["tp"]
        d_cap = opt_metrics["capex"] - base_metrics["capex"]
        d_wip = opt_metrics["wip"] - base_metrics["wip"]
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Weekly Net Profit", f"£{opt_metrics['profit']:,.0f}", f"£{d_prof:,.0f} vs Base")
        c2.metric("Weekly Throughput", f"{opt_metrics['tp']:.0f} units", f"{d_tp:.0f} vs Base")
        c3.metric("Avg WIP Holding", f"{opt_metrics['wip']:.0f} units", f"{d_wip:.0f} vs Base", delta_color="inverse")
        c4.metric("Total CAPEX Deployed", f"£{opt_metrics['capex']:,.0f}", f"£{d_cap:,.0f} change", delta_color="inverse")

    with t2:
        v_base, v_opt = st.tabs(["📊 Baseline State", "🚀 Optimized State"])
        with v_base:
            st.graphviz_chart(generate_dag_vsm(edited_nodes, edited_edges, base_q_data), use_container_width=True)
        with v_opt:
            st.graphviz_chart(generate_dag_vsm(current_nodes, edited_edges, opt_q_data), use_container_width=True)

    with t3:
        st.subheader("Physics Verification: WIP Queues Over Time")
        st.info("Notice how the Optimized limits force the lines to 'flatline' at their maximum capacity, completely eliminating runaway inventory spikes.")
        df_base = pd.DataFrame(base_q_data).melt(id_vars="Time (Mins)", var_name="Queue", value_name="Parts")
        df_base["State"] = "Baseline"
        df_opt = pd.DataFrame(opt_q_data).melt(id_vars="Time (Mins)", var_name="Queue", value_name="Parts")
        df_opt["State"] = "Optimized"
        
        fig = px.line(pd.concat([df_base, df_opt]), x="Time (Mins)", y="Parts", color="Queue", line_dash="State")
        st.plotly_chart(fig, use_container_width=True)    "Max_WIP_Limit": [9999, 9999, 50, 50, 100, 9999, 9999], # Finite capacity limits!
    "Mean_Mins": [5.0, 3.0, 8.0, 6.0, 12.0, 20.0, 0.0],
    "StdDev_Mins": [0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 0.0],
    "Yield_Qty": [1, 1, 1, 1, 1, 50, 50],
    "CAPEX_Base": [0, 0, 150000, 120000, 85000, 40000, 0],
    "OPEX_Weekly": [0, 0, 2000, 2500, 3000, 1500, 0],
    "Optimize_Qty": [False, False, True, False, True, False, False], 
    "Optimize_Limit": [False, False, True, True, True, False, False] # The AI can now test Kanban limits
)

edited_nodes = st.data_editor(default_nodes, num_rows="dynamic", use_container_width=True)

st.subheader("2️⃣ Factory Edges (Bill of Materials & Routing)")
default_edges = pd.DataFrame({
    "From_Node": ["Raw_Metal", "Raw_Plastic", "Milling", "Molding", "Assembly", "Shipping"],
    "To_Node": ["Milling", "Molding", "Assembly", "Assembly", "Shipping", "Finished_Goods"],
    "Qty_Required": [1, 1, 1, 2, 50, 50] 
})

edited_edges = st.data_editor(default_edges, num_rows="dynamic", use_container_width=True)

# --- CORE SIMULATION ENGINE ---
def evaluate_network(nodes_df, edges_df, num_runs=10, return_queues=False):
    run_profits, run_tps, run_wips = [], [], []
    sample_q_data = []

    total_capex = sum([row["Machines_Qty"] * row["CAPEX_Base"] for _, row in nodes_df[nodes_df["Type"]=="Machine"].iterrows()])
    total_opex = sum([row["Machines_Qty"] * row["OPEX_Weekly"] for _, row in nodes_df[nodes_df["Type"]=="Machine"].iterrows()])
    weekly_depr = (total_capex / 10.0) / 52.0

    def universal_node_process(env, node_id, node_type, mean_t, std_t, yield_qty, buffers, machines):
        inputs = [(e["From_Node"], int(e["Qty_Required"])) for _, e in edges_df.iterrows() if e["To_Node"] == node_id]

        while True:
            if node_type == "Source":
                yield env.timeout(max(0.1, random.gauss(mean_t, std_t)))
                # The yield statement here means "Pause this process until there is room in the buffer"
                yield buffers[node_id].put(yield_qty) 
                
            elif node_type == "Machine":
                if inputs:
                    get_events = [buffers[in_node].get(req_qty) for in_node, req_qty in inputs]
                    yield env.all_of(get_events)
                
                with machines[node_id].request() as req:
                    yield req
                    yield env.timeout(max(0.1, random.gauss(mean_t, std_t)))
                
                # MACHINE BLOCKING PHYSICS: If buffer is full, the machine sleeps here!
                yield buffers[node_id].put(yield_qty)
                
            elif node_type == "Sink":
                if inputs:
                    get_events = [buffers[in_node].get(req_qty) for in_node, req_qty in inputs]
                    yield env.all_of(get_events)
                    yield buffers[node_id].put(yield_qty)
                    yield env.timeout(0) 
                else:
                    break

    def monitor_network(env, buffers, q_data, save_sample):
        while True:
            total_wip = sum([buf.level for nid, buf in buffers.items() if nodes_df[nodes_df["Node_ID"]==nid]["Type"].values[0] != "Sink"])
            wip_snapshot.append(total_wip)
            if save_sample:
                row = {"Time (Mins)": env.now}
                for nid, buf in buffers.items(): row[f"{nid}_WIP"] = buf.level
                q_data.append(row)
            yield env.timeout(5)

    for r_idx in range(num_runs):
        env = simpy.Environment()
        buffers, machines = {}, {}
        wip_snapshot = []
        save_sample = return_queues and (r_idx == num_runs - 1)
        
        for _, row in nodes_df.iterrows():
            nid = row["Node_ID"]
            # Apply the Kanban limits from the table
            b_limit = int(row["Max_WIP_Limit"]) if row["Max_WIP_Limit"] > 0 else 99999
            buffers[nid] = simpy.Container(env, init=0, capacity=b_limit)
            if row["Type"] == "Machine":
                machines[nid] = simpy.Resource(env, capacity=int(max(1, row["Machines_Qty"])))

        for _, row in nodes_df.iterrows():
            env.process(universal_node_process(env, row["Node_ID"], row["Type"], row["Mean_Mins"], row["StdDev_Mins"], row["Yield_Qty"], buffers, machines))
            
        env.process(monitor_network(env, buffers, sample_q_data, save_sample))
        env.run(until=sim_time)
        
        tp_val = sum([buffers[nid].level for nid in nodes_df[nodes_df["Type"]=="Sink"]["Node_ID"]])
        wip_val = np.mean(wip_snapshot) if wip_snapshot else 0
        
        net_profit = (tp_val * rev_unit) - (tp_val * rm_cost) - total_opex - (wip_val * wip_cost) - weekly_depr
        
        run_profits.append(net_profit)
        run_tps.append(tp_val)
        run_wips.append(wip_val)

    res = {"profit": np.mean(run_profits), "tp": np.mean(run_tps), "wip": np.mean(run_wips), "capex": total_capex}
    if return_queues: return res, sample_q_data
    return res

# --- GRAPHVIZ GENERATOR ---
def generate_dag_vsm(nodes_df, edges_df, q_data):
    dot = graphviz.Digraph(node_attr={'shape': 'box', 'style': 'filled', 'color': '#E1E4E8', 'fontname': 'Helvetica'})
    dot.attr(rankdir='LR', splines='ortho')
    df_q = pd.DataFrame(q_data) if q_data else None
    
    for _, row in nodes_df.iterrows():
        nid, ntype, qty = row["Node_ID"], row["Type"], int(row["Machines_Qty"])
        lim = row["Max_WIP_Limit"]
        final_wip = df_q[f"{nid}_WIP"].iloc[-1] if df_q is not None else 0
        
        if ntype == "Source":
            dot.node(f'{nid}_Src', f'{nid}\n(Source)\nμ={row["Mean_Mins"]}m', color='#cce5ff', shape='folder')
            dot.node(f'{nid}_Buf', f'{nid} Buffer\nQty: {final_wip} / {lim}', color='#fff3cd', shape='cylinder')
            dot.edge(f'{nid}_Src', f'{nid}_Buf')
        elif ntype == "Sink":
            dot.node(nid, f'{nid}\n(Sink)\nTotal Finished: {final_wip}', color='#d4edda', shape='folder')
        else:
            # Highlight buffers that are red-lining (hitting their kanban limit)
            buf_color = '#f8d7da' if final_wip >= lim else '#fff3cd'
            dot.node(f'{nid}_Buf', f'{nid} Output\nQty: {final_wip} / {lim}', color=buf_color, shape='cylinder')
            
            with dot.subgraph(name=f'cluster_{nid}') as s:
                s.attr(style='invis')
                for i in range(qty):
                    s.node(f'{nid}_M{i}', f'{nid} {i+1}', color='#e2e3e5')
                    s.edge(f'{nid}_M{i}', f'{nid}_Buf')

    for _, edge in edges_df.iterrows():
        frm, to, req = edge["From_Node"], edge["To_Node"], edge["Qty_Required"]
        to_type = nodes_df[nodes_df["Node_ID"] == to]["Type"].values[0]
        
        if to_type == "Machine":
            qty = int(nodes_df[nodes_df["Node_ID"] == to]["Machines_Qty"].values[0])
            merge_id = f'merge_{frm}_{to}'
            dot.node(merge_id, '', shape='point', width='0')
            dot.edge(f'{frm}_Buf', merge_id, label=f' Pull: {req}', fontcolor='#0056b3', color='#0056b3')
            for i in range(qty): dot.edge(merge_id, f'{to}_M{i}', color='#0056b3')
        elif to_type == "Sink":
            dot.edge(f'{frm}_Buf', to, label=f' Pull: {req}', fontcolor='#2ca02c', color='#2ca02c')
            
    return dot

# --- HILL CLIMBING 2D OPTIMIZER ---
if run_opt:
    with st.spinner(f"Evaluating Client Baseline ({final_runs} Runs)..."):
        base_metrics, base_q_data = evaluate_network(edited_nodes, edited_edges, num_runs=final_runs, return_queues=True)
        
    st_progress = st.empty()
    st_log = st.empty()
    
    current_nodes = edited_nodes.copy()
    best_profit = base_metrics["profit"]
    search_log = ["🔍 **Commencing 2D Financial & Kanban Search...**"]
    
    opt_qty_idx = current_nodes[(current_nodes["Type"] == "Machine") & (current_nodes["Optimize_Qty"] == True)].index.tolist()
    opt_lim_idx = current_nodes[current_nodes["Optimize_Limit"] == True].index.tolist()

    for step in range(8): # Increased steps for 2D search space
        st_progress.info(f"Optimization Step {step+1}: Evaluating Kanban & Capacity neighbors...")
        neighbors = []
        
        # Dimension 1: Search Machine Quantities
        for idx in opt_qty_idx:
            if current_nodes.at[idx, "Machines_Qty"] < 10:
                n_df = current_nodes.copy(); n_df.at[idx, "Machines_Qty"] += 1; neighbors.append(n_df)
            if current_nodes.at[idx, "Machines_Qty"] > 1:
                n_df = current_nodes.copy(); n_df.at[idx, "Machines_Qty"] -= 1; neighbors.append(n_df)
                
        # Dimension 2: Search Kanban Limits (Increments of 5)
        for idx in opt_lim_idx:
            lim = current_nodes.at[idx, "Max_WIP_Limit"]
            if lim < 500: # Ceiling to prevent runaway loops
                n_df = current_nodes.copy(); n_df.at[idx, "Max_WIP_Limit"] = lim + 5; neighbors.append(n_df)
            if lim > 5: # Floor to prevent seizing the factory
                n_df = current_nodes.copy(); n_df.at[idx, "Max_WIP_Limit"] = lim - 5; neighbors.append(n_df)
                
        found_better = False
        for n_df in neighbors:
            m = evaluate_network(n_df, edited_edges, num_runs=eval_runs)
            if m["profit"] > best_profit:
                best_profit = m["profit"]
                current_nodes = n_df
                found_better = True
                search_log.append(f"✅ Improved configuration found. Est. Profit: £{m['profit']:,.0f}/wk")
                st_log.markdown("\n".join(search_log))
                
        if not found_better:
            search_log.append("🛑 Local Maxima Found. Assets and Limits are financially optimal.")
            st_log.markdown("\n".join(search_log))
            break
            
    st_progress.empty()
    st_log.empty()
    
    with st.spinner(f"Verifying Optimal Configuration ({final_runs} High-Fidelity Runs)..."):
        opt_metrics, opt_q_data = evaluate_network(current_nodes, edited_edges, num_runs=final_runs, return_queues=True)

    # --- UI DASHBOARDS ---
    t1, t2, t3 = st.tabs(["🏆 Gap Analysis", "🗺️ Value Stream Maps", "📈 Queue Physics"])

    with t1:
        st.subheader("Consulting Scorecard: Baseline vs. Optimized")
        d_prof = opt_metrics["profit"] - base_metrics["profit"]
        d_tp = opt_metrics["tp"] - base_metrics["tp"]
        d_cap = opt_metrics["capex"] - base_metrics["capex"]
        d_wip = opt_metrics["wip"] - base_metrics["wip"]
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Weekly Net Profit", f"£{opt_metrics['profit']:,.0f}", f"£{d_prof:,.0f} vs Base")
        c2.metric("Weekly Throughput", f"{opt_metrics['tp']:.0f} units", f"{d_tp:.0f} vs Base")
        c3.metric("Avg WIP Holding", f"{opt_metrics['wip']:.0f} units", f"{d_wip:.0f} vs Base", delta_color="inverse")
        c4.metric("Total CAPEX Deployed", f"£{opt_metrics['capex']:,.0f}", f"£{d_cap:,.0f} change", delta_color="inverse")

    with t2:
        v_base, v_opt = st.tabs(["📊 Baseline State", "🚀 Optimized State"])
        with v_base:
            st.graphviz_chart(generate_dag_vsm(edited_nodes, edited_edges, base_q_data), use_container_width=True)
        with v_opt:
            st.graphviz_chart(generate_dag_vsm(current_nodes, edited_edges, opt_q_data), use_container_width=True)

    with t3:
        st.subheader("Physics Verification: WIP Queues Over Time")
        st.info("Notice how the Optimized limits force the lines to 'flatline' at their maximum capacity, completely eliminating runaway inventory spikes.")
        df_base = pd.DataFrame(base_q_data).melt(id_vars="Time (Mins)", var_name="Queue", value_name="Parts")
        df_base["State"] = "Baseline"
        df_opt = pd.DataFrame(opt_q_data).melt(id_vars="Time (Mins)", var_name="Queue", value_name="Parts")
        df_opt["State"] = "Optimized"
        
        fig = px.line(pd.concat([df_base, df_opt]), x="Time (Mins)", y="Parts", color="Queue", line_dash="State")
        st.plotly_chart(fig, use_container_width=True)
