import streamlit as st
import simpy
import random
import pandas as pd
import plotly.express as px
import numpy as np
import graphviz
import io

st.set_page_config(page_title="DES Dynamic Factory Twin", layout="wide")

st.title("🏭 Dynamic Sim-Opt: E2E Network Twin")
st.markdown("This engine supports a dynamic number of stages. Upload your Excel baseline or use the editable table below to add/remove processes. The Hill-Climbing AI will optimize the capacities and speed upgrades for the entire network.")

# --- DATA GENERATION & I/O ---
def generate_template():
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        pd.DataFrame({
            "Parameter": ["Arrival_Rate_Mins", "Revenue_per_Unit", "RM_Cost_per_Unit", "WIP_Holding_Cost_per_Unit_Week"],
            "Value": [5.0, 500.0, 150.0, 15.0]
        }).to_excel(writer, sheet_name="System_Variables", index=False)
        
        pd.DataFrame({
            "Stage_Name": ["Milling", "Assembly", "QA"],
            "Qty_Machines": [2, 2, 1],
            "Mean_Mins": [8.0, 12.0, 4.0],
            "StdDev_Mins": [1.0, 2.0, 0.5],
            "CAPEX_Base": [150000, 85000, 40000],
            "OPEX_Weekly": [2000, 3000, 1500]
        }).to_excel(writer, sheet_name="Routing_Stages", index=False)
    return output.getvalue()

with st.sidebar:
    st.header("📥 Data Management")
    st.download_button("📥 Download Excel Template", data=generate_template(), file_name="Factory_Baseline.xlsx")
    uploaded_file = st.file_uploader("Upload Baseline (.xlsx)", type=["xlsx"])
    
    st.markdown("---")
    st.header("⚙️ System Variables")
    if uploaded_file:
        sys_df = pd.read_excel(uploaded_file, sheet_name="System_Variables").set_index("Parameter")
        arr_rate = st.number_input("RM Arrival Rate (mins)", value=float(sys_df.loc["Arrival_Rate_Mins", "Value"]))
        rev_unit = st.number_input("Revenue/Unit (£)", value=float(sys_df.loc["Revenue_per_Unit", "Value"]))
        rm_cost = st.number_input("RM Cost/Unit (£)", value=float(sys_df.loc["RM_Cost_per_Unit", "Value"]))
        wip_cost = st.number_input("WIP Holding Penalty (£/wk)", value=float(sys_df.loc["WIP_Holding_Cost_per_Unit_Week", "Value"]))
    else:
        arr_rate = st.number_input("RM Arrival Rate (mins)", value=5.0)
        rev_unit = st.number_input("Revenue/Unit (£)", value=500.0)
        rm_cost = st.number_input("RM Cost/Unit (£)", value=150.0)
        wip_cost = st.number_input("WIP Holding Penalty (£/wk)", value=15.0)
        
    sim_time = 40 * 60 # 40 hours
    run_analysis = st.button("🚀 Run AI Gap Analysis", type="primary", use_container_width=True)

# --- DYNAMIC STAGE EDITOR ---
st.subheader("🛠️ Process Routing (Editable)")
if uploaded_file:
    default_stages = pd.read_excel(uploaded_file, sheet_name="Routing_Stages")
else:
    default_stages = pd.DataFrame({
        "Stage_Name": ["Milling", "Assembly", "QA"],
        "Qty_Machines": [2, 2, 1],
        "Mean_Mins": [8.0, 12.0, 4.0],
        "StdDev_Mins": [1.0, 2.0, 0.5],
        "CAPEX_Base": [150000, 85000, 40000],
        "OPEX_Weekly": [2000, 3000, 1500]
    })

# The user can add/delete rows right here in the UI
edited_stages = st.data_editor(default_stages, num_rows="dynamic", use_container_width=True)
stage_names = edited_stages["Stage_Name"].tolist()
num_stages = len(stage_names)

# --- CORE DES EVALUATION FUNCTION (DYNAMIC) ---
def evaluate_network(qtys, speeds, num_runs=50, return_queues=False):
    """
    qtys: list of machine counts per stage
    speeds: list of 0 (standard) or 1 (high-speed) per stage
    """
    mu_mods = [1.0 if s==0 else 0.7 for s in speeds]
    cap_mods = [1.0 if s==0 else 2.0 for s in speeds]
    op_mods = [1.0 if s==0 else 1.5 for s in speeds]
    
    mc_tp = []
    mc_lt = []
    wip_counts = []
    sample_q_data = []

    def part_journey(env, resources):
        arr = env.now
        for i in range(num_stages):
            with resources[i].request() as req:
                yield req
                mean_time = edited_stages.loc[i, "Mean_Mins"] * mu_mods[i]
                std_time = edited_stages.loc[i, "StdDev_Mins"]
                yield env.timeout(max(0.1, random.gauss(mean_time, std_time)))
        mc_tp_run.append(1)
        mc_lt_run.append(env.now - arr)

    def part_generator(env, resources):
        while True:
            yield env.timeout(random.expovariate(1.0 / arr_rate))
            env.process(part_journey(env, resources))

    def monitor_queues(env, resources, q_data, save_sample):
        while True:
            total_wip = sum([len(r.queue) for r in resources])
            run_wip.append(total_wip)
            if save_sample:
                row = {"Time": env.now}
                for i in range(num_stages):
                    row[f"{stage_names[i]} Queue"] = len(resources[i].queue)
                q_data.append(row)
            yield env.timeout(5)

    for r_idx in range(num_runs):
        env = simpy.Environment()
        resources = [simpy.Resource(env, capacity=qtys[i]) for i in range(num_stages)]
        mc_tp_run, mc_lt_run, run_wip = [], [], []
        
        save_sample = return_queues and (r_idx == num_runs - 1)
        
        env.process(part_generator(env, resources))
        env.process(monitor_queues(env, resources, sample_q_data, save_sample))
        env.run(until=sim_time)
        
        mc_tp.append(len(mc_tp_run))
        if mc_lt_run: mc_lt.extend(mc_lt_run)
        wip_counts.append(np.mean(run_wip) if run_wip else 0)

    avg_tp = np.mean(mc_tp)
    avg_wip = np.mean(wip_counts)
    avg_lt = np.mean(mc_lt) if mc_lt else 0
    
    # Financials
    total_capex = sum([qtys[i] * edited_stages.loc[i, "CAPEX_Base"] * cap_mods[i] for i in range(num_stages)])
    total_opex = sum([qtys[i] * edited_stages.loc[i, "OPEX_Weekly"] * op_mods[i] for i in range(num_stages)])
    
    weekly_rev = avg_tp * rev_unit
    weekly_rm_tot = avg_tp * rm_cost
    weekly_wip_tot = avg_wip * wip_cost
    weekly_depr = total_capex / 520.0 
    
    net_profit = weekly_rev - weekly_rm_tot - total_opex - weekly_wip_tot - weekly_depr
    
    # Calculate buffer queue averages for the specific sample run for VSM plotting
    q_avgs = []
    if return_queues and sample_q_data:
        df_q = pd.DataFrame(sample_q_data)
        for i in range(num_stages):
            q_avgs.append(df_q[f"{stage_names[i]} Queue"].mean())
    else:
        q_avgs = [0] * num_stages
    
    res = {"profit": net_profit, "tp": avg_tp, "wip": avg_wip, "lt": avg_lt, "capex": total_capex, "opex": total_opex, "q_avgs": q_avgs}
    if return_queues: return res, sample_q_data
    return res

# --- GRAPHVIZ VSM GENERATOR (DYNAMIC & PARALLEL) ---
def generate_vsm(qtys, speeds, metrics, view_type="ops"):
    mu_mods = [1.0 if s==0 else 0.7 for s in speeds]
    cap_mods = [1.0 if s==0 else 2.0 for s in speeds]
    op_mods = [1.0 if s==0 else 1.5 for s in speeds]
    q_avgs = metrics["q_avgs"]
    tp = max(1, metrics["tp"])
    
    dot = graphviz.Digraph(node_attr={'shape': 'box', 'style': 'filled', 'color': '#E1E4E8', 'fontname': 'Helvetica'})
    dot.attr(rankdir='LR', splines='ortho')
    
    # Starting Node
    if view_type == "ops":
        dot.node('IN', f'Raw Materials\nArrival: {arr_rate}m', color='#cce5ff', shape='folder')
    else:
        dot.node('IN', f'Raw Materials\n£{rm_cost:.2f} / unit', color='#cce5ff', shape='folder')
        
    prev_node = 'IN'
    total_cost_pu = rm_cost
    
    for i in range(num_stages):
        buffer_id = f'B{i}'
        b_pu = (q_avgs[i] * wip_cost) / tp
        total_cost_pu += b_pu
        
        # Buffer Node
        if view_type == "ops":
            dot.node(buffer_id, f'WIP Buffer {i+1}\nAvg: {q_avgs[i]:.1f} parts', color='#fff3cd', shape='cylinder')
        else:
            dot.node(buffer_id, f'WIP Buffer {i+1}\nPenalty: £{b_pu:.2f} / unit', color='#f8d7da', shape='cylinder')
            
        dot.edge(prev_node, buffer_id)
        
        # Parallel Processing Nodes
        with dot.subgraph() as s:
            s.attr(rank='same')
            m_op_pu = (qtys[i] * edited_stages.loc[i, "OPEX_Weekly"] * op_mods[i]) / tp if qtys[i] > 0 else 0
            m_cap_pu = ((qtys[i] * edited_stages.loc[i, "CAPEX_Base"] * cap_mods[i]) / 520) / tp if qtys[i] > 0 else 0
            total_cost_pu += (m_op_pu + m_cap_pu)
            
            for m in range(qtys[i]):
                node_id = f'S{i}_M{m}'
                if view_type == "ops":
                    mean_val = edited_stages.loc[i, "Mean_Mins"] * mu_mods[i]
                    lbl = f'{stage_names[i]} {m+1}\n{"⚡ High-Speed" if speeds[i] else "Standard"}\nμ={mean_val:.1f}m, σ={edited_stages.loc[i, "StdDev_Mins"]}m'
                else:
                    lbl = f'{stage_names[i]} {m+1}\nOPEX: £{m_op_pu/qtys[i] if qtys[i]>0 else 0:.2f} / unit\nDepr: £{m_cap_pu/qtys[i] if qtys[i]>0 else 0:.2f} / unit'
                s.node(node_id, lbl, color='#e2e3e5' if speeds[i]==0 else '#ffeeba')
                dot.edge(buffer_id, node_id)
                
        # To merge parallel nodes back, we use an invisible node or direct to next buffer.
        # Directing to next buffer happens in the next loop, but we need a merge point.
        merge_id = f'Merge{i}'
        dot.node(merge_id, '', shape='point', width='0')
        for m in range(qtys[i]):
            dot.edge(f'S{i}_M{m}', merge_id)
            
        prev_node = merge_id

    # Ending Node
    net_margin_pu = rev_unit - total_cost_pu
    if view_type == "ops":
        dot.node('OUT', f'Finished Goods\nThroughput: {metrics["tp"]:.0f}/wk\nLead Time: {metrics["lt"]:.1f}m', color='#d4edda', shape='folder')
    else:
        dot.node('OUT', f'Finished Goods\nCost: £{total_cost_pu:.2f} / unit\nMargin: £{net_margin_pu:.2f} / unit', color='#d4edda', shape='folder')
        
    dot.edge(prev_node, 'OUT')
    return dot

# --- EXECUTION & HILL CLIMBER ---
if run_analysis:
    base_qtys = edited_stages["Qty_Machines"].tolist()
    base_speeds = [0] * num_stages # Baseline is all 0 (Standard speed)
    
    with st.spinner("Evaluating Client Baseline (50 Runs)..."):
        base_metrics, base_q_data = evaluate_network(base_qtys, base_speeds, num_runs=50, return_queues=True)
        
    st_progress = st.empty()
    st_log = st.empty()
    
    curr_qtys = list(base_qtys)
    curr_speeds = list(base_speeds)
    best_profit = base_metrics["profit"]
    search_log = ["🔍 **Commencing Dynamic Heuristic Search...**"]
    
    for step in range(5):
        st_progress.info(f"Optimization Step {step+1}: Evaluating neighbors...")
        neighbors = []
        
        # Generate Qty +/- 1
        for i in range(num_stages):
            for delta in [-1, 1]:
                nq = list(curr_qtys)
                nq[i] += delta
                if 1 <= nq[i] <= 10: neighbors.append((nq, curr_speeds))
        # Generate Speed toggles
        for i in range(num_stages):
            ns = list(curr_speeds)
            ns[i] = 1 if curr_speeds[i] == 0 else 0
            neighbors.append((curr_qtys, ns))
            
        found_better = False
        for nq, ns in neighbors:
            m = evaluate_network(nq, ns, num_runs=10) # Fast eval
            if m["profit"] > best_profit:
                best_profit = m["profit"]
                curr_qtys, curr_speeds = nq, ns
                found_better = True
                search_log.append(f"✅ Found better state. Est. Profit: £{m['profit']:,.0f}")
                st_log.markdown("\n".join(search_log))
                
        if not found_better:
            search_log.append("🛑 Local Maxima Found. Terminating search.")
            st_log.markdown("\n".join(search_log))
            break
            
    st_progress.empty()
    st_log.empty()
    
    with st.spinner("Verifying Optimal Configuration (50 Runs)..."):
        opt_metrics, opt_q_data = evaluate_network(curr_qtys, curr_speeds, num_runs=50, return_queues=True)

    # --- UI DASHBOARDS ---
    t1, t2, t3, t4 = st.tabs(["🏆 Executive Gap Analysis", "🗺️ Value Stream Maps", "📈 Queue Physics", "📥 CFO Audit Ledger"])

    with t1:
        st.subheader("Consulting Scorecard: Baseline vs. Optimized")
        d_prof = opt_metrics["profit"] - base_metrics["profit"]
        d_tp = opt_metrics["tp"] - base_metrics["tp"]
        d_lt = opt_metrics["lt"] - base_metrics["lt"]
        d_cap = opt_metrics["capex"] - base_metrics["capex"]
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Weekly Net Profit", f"£{opt_metrics['profit']:,.0f}", f"£{d_prof:,.0f} vs Base")
        c2.metric("Weekly Throughput", f"{opt_metrics['tp']:.0f} units", f"{d_tp:.0f} vs Base")
        c3.metric("Cycle Time (Lead Time)", f"{opt_metrics['lt']:.1f} mins", f"{d_lt:.1f} mins", delta_color="inverse")
        c4.metric("Total CAPEX Deployed", f"£{opt_metrics['capex']:,.0f}", f"£{d_cap:,.0f} new spend", delta_color="inverse")
        
        st.markdown("---")
        if d_cap > 0 and d_prof > 0:
            marginal_payback = (d_cap / (d_prof * 52)) * 12
            st.success(f"**Investment Thesis:** The optimizer suggests deploying an additional **£{d_cap:,.0f}** in CAPEX. This unlocks **£{d_prof*52:,.0f}** in annualized marginal profit, representing a payback period of **{marginal_payback:.1f} months** on the new capital.")
        elif d_cap < 0 and d_prof > 0:
            st.success(f"**Lean Thesis:** The optimizer found a more efficient state that requires **£{-d_cap:,.0f} LESS** CAPEX while increasing weekly profit. Assets should be divested or repurposed.")

    with t2:
        st.markdown("Toggle between Physics (Flow) and Financial (Activity-Based Costing) views. The parallel routing visually demonstrates capacity width.")
        v_base, v_opt = st.tabs(["📊 Baseline State Diagrams", "🚀 Optimized State Diagrams"])
        
        with v_base:
            st.markdown("### Operations & Physics Flow")
            st.graphviz_chart(generate_vsm(base_qtys, base_speeds, base_metrics, "ops"), use_container_width=True)
            st.markdown("### Financial Unit Cost Waterfall")
            st.graphviz_chart(generate_vsm(base_qtys, base_speeds, base_metrics, "fin"), use_container_width=True)
            
        with v_opt:
            st.markdown("### Operations & Physics Flow")
            st.graphviz_chart(generate_vsm(curr_qtys, curr_speeds, opt_metrics, "ops"), use_container_width=True)
            st.markdown("### Financial Unit Cost Waterfall")
            st.graphviz_chart(generate_vsm(curr_qtys, curr_speeds, opt_metrics, "fin"), use_container_width=True)

    with t3:
        st.subheader("Physics Verification: WIP Queues Over Time")
        df_base = pd.DataFrame(base_q_data).melt(id_vars="Time", var_name="Queue", value_name="Parts")
        df_base["State"] = "Baseline"
        df_opt = pd.DataFrame(opt_q_data).melt(id_vars="Time", var_name="Queue", value_name="Parts")
        df_opt["State"] = "Optimized"
        
        fig = px.line(pd.concat([df_base, df_opt]), x="Time", y="Parts", color="Queue", line_dash="State", title="Bottleneck Migration Comparison")
        st.plotly_chart(fig, use_container_width=True)

    with t4:
        st.subheader("Export Analysis Data")
        comp_data = []
        for i in range(num_stages):
            comp_data.append({
                "Stage": stage_names[i],
                "Baseline Config": f"{base_qtys[i]}x Standard",
                "Optimized Config": f"{curr_qtys[i]}x {'High-Speed' if curr_speeds[i] else 'Standard'}"
            })
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            pd.DataFrame(comp_data).to_excel(writer, sheet_name="Stage_Deltas", index=False)
            pd.DataFrame([base_metrics]).to_excel(writer, sheet_name="Base_Financials", index=False)
            pd.DataFrame([opt_metrics]).to_excel(writer, sheet_name="Opt_Financials", index=False)
            pd.DataFrame(base_q_data).to_excel(writer, sheet_name="Baseline_Queue_Raw", index=False)
            pd.DataFrame(opt_q_data).to_excel(writer, sheet_name="Optimal_Queue_Raw", index=False)
            
        st.download_button("📥 Export Boardroom Audit (.xlsx)", data=output.getvalue(), file_name="Dynamic_Twin_Gap_Analysis.xlsx")
