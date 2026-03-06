import streamlit as st
import simpy
import random
import pandas as pd
import plotly.express as px
import numpy as np
import graphviz
import time

st.set_page_config(page_title="DAG Factory Optimizer", layout="wide")

st.title("🏭 True MRP Kingman Sim-Opt Engine")
st.markdown("Step 1 uses algebraic **BOM Explosion (MRP)**. Step 2 uses **Kingman's Formula**. Step 3 uses **Monte Carlo**. The engine features a JIT kill-switch to halt production once demand is met.")

# --- SIDEBAR: FINANCIALS & SETTINGS ---
with st.sidebar:
    st.header("🎯 Demand & Kingman Target")
    weekly_demand = st.number_input("Actual Weekly Demand", value=500)
    target_max_q = st.slider("Target Max Queue (Kingman)", 2, 50, 10)
    
    st.markdown("---")
    st.header("💸 Financial Economics")
    rev_unit = st.number_input("Revenue per Unit (£)", value=500.0)
    rm_cost = st.number_input("Raw Material Cost/Unit (£)", value=150.0)
    wip_cost = st.number_input("WIP Holding Penalty/Unit/Wk (£)", value=15.0)
    tax_rate = 0.25 # Assuming 25% tax for ROIC
    
    st.markdown("---")
    st.header("⏱️ Computing Limits")
    sim_time = 40 * 60
    eval_runs = 3   
    final_runs = 15 
    
    run_opt = st.button("🚀 Run Algebraic Optimizer", type="primary", use_container_width=True)

# --- DYNAMIC TABLES ---
st.subheader("1️⃣ Factory Nodes (Machines, Stages & Constraints)")
default_nodes = pd.DataFrame({
    "Node_ID": ["Raw_Metal", "Raw_Plastic", "Milling", "Molding", "Assembly", "Shipping", "Finished_Goods"],
    "Macro_Stage": ["1. Sourcing", "1. Sourcing", "2. Machining", "2. Machining", "3. Assembly", "4. Outbound", "4. Outbound"], # TOP ROW STAGES
    "Type": ["Source", "Source", "Machine", "Machine", "Machine", "Machine", "Sink"],
    "Machines_Qty": [1, 1, 2, 3, 2, 1, 1],
    "Max_WIP_Limit": [9999, 9999, 50, 50, 100, 9999, 9999], 
    "Mean_Mins": [5.0, 3.0, 8.0, 6.0, 12.0, 20.0, 0.0],
    "StdDev_Mins": [0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 0.0],
    "Yield_Qty": [1, 1, 1, 1, 1, 50, 50],
    "CAPEX_Base": [0, 0, 150000, 120000, 85000, 40000, 0],
    "OPEX_Weekly": [0, 0, 2000, 2500, 3000, 1500, 0],
    "Optimize_Qty": [True, True, True, True, True, False, False], 
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

# --- CORE SIMULATION ENGINE WITH KILL SWITCH ---
def evaluate_network(nodes_df, edges_df, num_runs=3, return_queues=False, include_variance=True):
    run_profits, run_tps, run_wips, run_roics = [], [], [], []
    sample_q_data = []
    
    empirical_processed = {row["Node_ID"]: 0 for _, row in nodes_df.iterrows()}
    total_capex = sum([row["Machines_Qty"] * row["CAPEX_Base"] for _, row in nodes_df[nodes_df["Type"]=="Machine"].iterrows()])
    total_opex = sum([row["Machines_Qty"] * row["OPEX_Weekly"] for _, row in nodes_df[nodes_df["Type"]=="Machine"].iterrows()])
    weekly_depr = (total_capex / 10.0) / 52.0

    def universal_node_process(env, node_id, node_type, mean_t, std_t, yield_qty, buffers, machines):
        inputs = [(e["From_Node"], int(e["Qty_Required"])) for _, e in edges_df.iterrows() if e["To_Node"] == node_id]
        actual_std = std_t if include_variance else 0.0

        while True:
            if node_type == "Source":
                yield env.timeout(max(0.1, random.gauss(mean_t, actual_std)))
                yield buffers[node_id].put(yield_qty)
                empirical_processed[node_id] += yield_qty
            elif node_type == "Machine":
                if inputs:
                    get_events = [buffers[in_node].get(req_qty) for in_node, req_qty in inputs]
                    yield env.all_of(get_events)
                with machines[node_id].request() as req:
                    yield req
                    yield env.timeout(max(0.1, random.gauss(mean_t, actual_std)))
                yield buffers[node_id].put(yield_qty)
                empirical_processed[node_id] += yield_qty
            elif node_type == "Sink":
                if inputs:
                    get_events = [buffers[in_node].get(req_qty) for in_node, req_qty in inputs]
                    yield env.all_of(get_events)
                    yield buffers[node_id].put(yield_qty)
                    empirical_processed[node_id] += yield_qty
                    yield env.timeout(0) 
                else:
                    break

    def monitor_network(env, buffers, q_data, save_sample, target_met_event):
        while True:
            total_wip = sum([buf.level for nid, buf in buffers.items() if nodes_df[nodes_df["Node_ID"]==nid]["Type"].values[0] != "Sink"])
            wip_snapshot.append(total_wip)
            
            # KILL SWITCH LOGIC
            total_fg = sum([buf.level for nid, buf in buffers.items() if nodes_df[nodes_df["Node_ID"]==nid]["Type"].values[0] == "Sink"])
            if total_fg >= weekly_demand and not target_met_event.triggered:
                target_met_event.succeed() # Triggers the simulation to halt
                
            if save_sample:
                row = {"Time (Mins)": env.now}
                for nid, buf in buffers.items(): row[f"{nid}_WIP"] = buf.level
                q_data.append(row)
            yield env.timeout(5)

    for r_idx in range(num_runs):
        env = simpy.Environment()
        buffers, machines = {}, {}
        wip_snapshot = []
        target_met = env.event() # The Kill Switch Event
        save_sample = return_queues and (r_idx == num_runs - 1)
        
        for _, row in nodes_df.iterrows():
            nid = row["Node_ID"]
            b_limit = int(row["Max_WIP_Limit"]) if row["Max_WIP_Limit"] > 0 else 99999
            buffers[nid] = simpy.Container(env, init=0, capacity=b_limit)
            if row["Type"] == "Machine":
                machines[nid] = simpy.Resource(env, capacity=int(max(1, row["Machines_Qty"])))

        for _, row in nodes_df.iterrows():
            loop_count = int(max(1, row["Machines_Qty"])) if row["Type"] == "Source" else 1
            for _ in range(loop_count):
                env.process(universal_node_process(env, row["Node_ID"], row["Type"], row["Mean_Mins"], row["StdDev_Mins"], row["Yield_Qty"], buffers, machines))
            
        env.process(monitor_network(env, buffers, sample_q_data, save_sample, target_met))
        
        # Halt simulation if 40 hours expire OR the kill-switch triggers
        env.run(until=env.any_of([env.timeout(sim_time), target_met]))
        
        tp_val = sum([buffers[nid].level for nid in nodes_df[nodes_df["Type"]=="Sink"]["Node_ID"]])
        wip_val = np.mean(wip_snapshot) if wip_snapshot else 0
        
        # Financial & ROIC Math
        sold_units = min(tp_val, weekly_demand)
        wk_rev = sold_units * rev_unit
        wk_rm = tp_val * rm_cost  
        
        net_profit = wk_rev - wk_rm - total_opex - (wip_val * wip_cost) - weekly_depr
        nopat = net_profit * (1 - tax_rate) if net_profit > 0 else net_profit
        
        invested_capital = total_capex + (wip_val * rm_cost) # CAPEX + Working Capital (Inventory)
        annualized_roic = ((nopat * 52) / invested_capital) * 100 if invested_capital > 0 else 0
        
        run_profits.append(net_profit)
        run_tps.append(tp_val)
        run_wips.append(wip_val)
        run_roics.append(annualized_roic)

    avg_processed = {k: v/num_runs for k, v in empirical_processed.items()}
    res = {"profit": np.mean(run_profits), "tp": np.mean(run_tps), "wip": np.mean(run_wips), "capex": total_capex, "roic": np.mean(run_roics), "processed_rates": avg_processed}
    if return_queues: return res, sample_q_data
    return res

# --- GRAPHVIZ GENERATOR WITH MACRO STAGES ---
def generate_dag_vsm(nodes_df, edges_df, q_data):
    dot = graphviz.Digraph(node_attr={'shape': 'box', 'style': 'filled', 'color': '#E1E4E8', 'fontname': 'Helvetica'})
    dot.attr(rankdir='LR', splines='ortho')
    df_q = pd.DataFrame(q_data) if q_data else None
    
    # Create Subgraphs for Macro Stages (The "Top Row")
    stages = nodes_df["Macro_Stage"].unique()
    
    for stage in stages:
        with dot.subgraph(name=f'cluster_{stage}') as c:
            c.attr(label=stage, style='dashed', color='gray', fontname='Helvetica', fontsize='14')
            
            stage_nodes = nodes_df[nodes_df["Macro_Stage"] == stage]
            for _, row in stage_nodes.iterrows():
                nid, ntype, qty = row["Node_ID"], row["Type"], int(row["Machines_Qty"])
                lim = row["Max_WIP_Limit"]
                final_wip = df_q[f"{nid}_WIP"].iloc[-1] if df_q is not None else 0
                
                if ntype == "Source":
                    lbl = f'{nid}\n(Source)\n[{qty}x Streams]\nμ={row["Mean_Mins"]}m'
                    c.node(f'{nid}_Src', lbl, color='#cce5ff', shape='folder')
                    c.node(f'{nid}_Buf', f'{nid} Buffer\nQty: {final_wip} / {lim}', color='#fff3cd', shape='cylinder')
                    c.edge(f'{nid}_Src', f'{nid}_Buf')
                elif ntype == "Sink":
                    c.node(nid, f'{nid}\n(Sink)\nTotal Finished: {final_wip}', color='#d4edda', shape='folder')
                else:
                    buf_color = '#f8d7da' if final_wip >= lim else '#fff3cd'
                    c.node(f'{nid}_Buf', f'{nid} Output\nQty: {final_wip} / {lim}', color=buf_color, shape='cylinder')
                    with c.subgraph(name=f'cluster_m_{nid}') as s:
                        s.attr(style='invis')
                        for i in range(qty):
                            m_lbl = f'{nid} {i+1}\nμ={row["Mean_Mins"]}m, σ={row["StdDev_Mins"]}m'
                            s.node(f'{nid}_M{i}', m_lbl, color='#e2e3e5')
                            s.edge(f'{nid}_M{i}', f'{nid}_Buf')

    # Draw Edges
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

# --- 3-STEP ALGEBRAIC KINGMAN OPTIMIZER ---
if run_opt:
    with st.spinner(f"Evaluating Client Baseline ({final_runs} Runs)..."):
        base_metrics, base_q_data = evaluate_network(edited_nodes, edited_edges, num_runs=final_runs, return_queues=True, include_variance=True)
        
    st_progress = st.empty()
    st_log = st.empty()
    current_nodes = edited_nodes.copy()
    
    # Audit Logs
    audit_mrp = []
    audit_kingman = []
    
    # --- ALGEBRAIC MRP DEMAND TRAVERSAL ---
    consumers = {nid: [] for nid in current_nodes["Node_ID"]}
    for _, e in edited_edges.iterrows():
        consumers[e["From_Node"]].append({"to": e["To_Node"], "req": e["Qty_Required"]})

    def get_algebraic_demand(node):
        if current_nodes[current_nodes["Node_ID"]==node]["Type"].values[0] == "Sink": return weekly_demand
        total_d = 0
        for cons in consumers[node]:
            c_node, c_req = cons["to"], cons["req"]
            c_yield = max(1, current_nodes[current_nodes["Node_ID"]==c_node]["Yield_Qty"].values[0])
            total_d += (get_algebraic_demand(c_node) / c_yield) * c_req
        return total_d

    # --- STEP 1: MRP ALGEBRA ---
    st_log.markdown("**STEP 1: Algebraic MRP BOM Explosion**")
    for idx, row in current_nodes.iterrows():
        if row["Optimize_Qty"]:
            nid = row["Node_ID"]
            target_d = get_algebraic_demand(nid)
            if row["Mean_Mins"] > 0:
                yield_per_mach = (sim_time / row["Mean_Mins"]) * row["Yield_Qty"]
                req_mach = int(np.ceil(target_d / yield_per_mach))
                current_nodes.at[idx, "Machines_Qty"] = max(1, req_mach)
                
                audit_mrp.append({"Node": nid, "Target Demand (Units)": target_d, "Yield per Machine": round(yield_per_mach,1), "Calculated Machines": max(1, req_mach)})

    # --- STEP 2: KINGMAN'S FORMULA ---
    st_log.markdown("**STEP 2: Kingman Equation Protective Sizing**")
    static_run = evaluate_network(current_nodes, edited_edges, num_runs=1, include_variance=False)
    empirical_rates = static_run["processed_rates"]
    
    for idx, row in current_nodes.iterrows():
        if row["Optimize_Qty"] and row["Type"] == "Machine":
            nid = row["Node_ID"]
            lam = empirical_rates[nid] / sim_time 
            mu = 1.0 / row["Mean_Mins"] if row["Mean_Mins"] > 0 else 1
            std = row["StdDev_Mins"]
            cv_a = 1.0 
            cv_s = std / row["Mean_Mins"] if row["Mean_Mins"] > 0 else 0
            
            initial_c = current_nodes.at[idx, "Machines_Qty"]
            
            while True:
                c = current_nodes.at[idx, "Machines_Qty"]
                rho = lam / (c * mu) if (c * mu) > 0 else 1
                if rho >= 1.0:
                    current_nodes.at[idx, "Machines_Qty"] += 1
                    continue
                lq = ((rho**(np.sqrt(2*(c+1)))) / (1 - rho)) * ((cv_a**2 + cv_s**2) / 2)
                
                if lq > target_max_q and c < 15:
                    current_nodes.at[idx, "Machines_Qty"] += 1
                else:
                    audit_kingman.append({"Node": nid, "Arrival Rate (λ)": round(lam, 2), "Process Rate (μ)": round(mu, 2), "Variance (CVs)": round(cv_s, 2), "Utilization (ρ)": f"{rho*100:.1f}%", "Est. Queue": round(lq, 1), "Final Machines": c})
                    break

    # --- STEP 3: DYNAMIC KANBAN OPTIMIZATION ---
    st_log.markdown("**STEP 3: Dynamic Kanban Limits Optimized.**")
    best_profit = evaluate_network(current_nodes, edited_edges, num_runs=eval_runs, include_variance=True)["profit"]
    opt_lim_idx = current_nodes[current_nodes["Optimize_Limit"] == True].index.tolist()

    for step in range(3): 
        st_progress.info(f"Step 3 (Iter {step+1}): Optimizing Kanban limits...")
        neighbors = []
        for idx in opt_lim_idx:
            lim = current_nodes.at[idx, "Max_WIP_Limit"]
            nid = current_nodes.at[idx, "Node_ID"]
            pulls = edited_edges[edited_edges["From_Node"] == nid]["Qty_Required"]
            floor_limit = max(5, int(pulls.max()) if not pulls.empty else 1)
            
            if lim < 500: n_df = current_nodes.copy(); n_df.at[idx, "Max_WIP_Limit"] = lim + 5; neighbors.append(n_df)
            if lim > floor_limit: n_df = current_nodes.copy(); n_df.at[idx, "Max_WIP_Limit"] = max(floor_limit, lim - 5); neighbors.append(n_df)
                
        found_better = False
        for n_df in neighbors:
            m = evaluate_network(n_df, edited_edges, num_runs=eval_runs, include_variance=True)
            if m["profit"] > best_profit:
                best_profit = m["profit"]
                current_nodes = n_df
                found_better = True
        if not found_better:
            break
            
    st_progress.empty()
    st_log.empty()
    
    with st.spinner(f"Verifying Final Configuration ({final_runs} Runs)..."):
        opt_metrics, opt_q_data = evaluate_network(current_nodes, edited_edges, num_runs=final_runs, return_queues=True, include_variance=True)

    # --- UI DASHBOARDS ---
    t1, t2, t3, t4 = st.tabs(["🏆 Gap Analysis", "🔍 Calculation Audit", "🗺️ Value Stream Maps", "📈 Queue Physics"])

    with t1:
        st.subheader("Consulting Scorecard: Baseline vs. Optimized")
        d_prof = opt_metrics["profit"] - base_metrics["profit"]
        d_tp = opt_metrics["tp"] - base_metrics["tp"]
        d_cap = opt_metrics["capex"] - base_metrics["capex"]
        d_roic = opt_metrics["roic"] - base_metrics["roic"]
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Weekly Net Profit", f"£{opt_metrics['profit']:,.0f}", f"£{d_prof:,.0f} vs Base")
        c2.metric("Weekly Throughput", f"{opt_metrics['tp']:.0f} units", f"{d_tp:.0f} vs Base")
        c3.metric("Annualized ROIC", f"{opt_metrics['roic']:.1f}%", f"{d_roic:.1f}% vs Base")
        c4.metric("Total CAPEX Deployed", f"£{opt_metrics['capex']:,.0f}", f"£{d_cap:,.0f} change", delta_color="inverse")

    with t2:
        st.subheader("Step 1: MRP Algebraic BOM Explosion")
        st.markdown("This calculates the absolute minimum machines required assuming zero variance.")
        st.dataframe(pd.DataFrame(audit_mrp), use_container_width=True)
        
        st.subheader("Step 2: Kingman's Protective Capacity Formula")
        st.markdown("This calculates the *extra* machines required to absorb variance and prevent infinite queues.")
        st.dataframe(pd.DataFrame(audit_kingman), use_container_width=True)

    with t3:
        st.graphviz_chart(generate_dag_vsm(current_nodes, edited_edges, opt_q_data), use_container_width=True)

    with t4:
        st.subheader("Optimized Queue Physics")
        st.markdown("Baseline data removed per request. Notice how the JIT kill-switch prevents late-stage inventory explosions once demand is met.")
        df_opt = pd.DataFrame(opt_q_data).melt(id_vars="Time (Mins)", var_name="Queue", value_name="Parts")
        fig = px.line(df_opt, x="Time (Mins)", y="Parts", color="Queue")
        st.plotly_chart(fig, use_container_width=True)
