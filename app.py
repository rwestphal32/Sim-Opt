import streamlit as st
import simpy
import random
import pandas as pd
import plotly.express as px
import numpy as np
import graphviz
import io

st.set_page_config(page_title="DES E2E Network Twin", layout="wide")

st.title("🏭 Dynamic Sim-Opt: E2E Financial Twin")
st.markdown("This engine models physical throughput against the full Cash Conversion Cycle. It now includes **95% Confidence Intervals (CI)** to prove statistical validity. Use the Fidelity Slider to see how increasing sample size tightens the error margin.")

# --- DATA GENERATION & I/O ---
def generate_template():
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        pd.DataFrame({
            "Parameter": ["Arrival_Rate_Mins", "Revenue_per_Unit", "RM_Cost_per_Unit", "Tax_Rate", "DSO_Days", "DPO_Days"],
            "Value": [5.0, 500.0, 150.0, 0.25, 45.0, 30.0]
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
    st.download_button("📥 Download E2E Template", data=generate_template(), file_name="E2E_Baseline.xlsx")
    uploaded_file = st.file_uploader("Upload Baseline (.xlsx)", type=["xlsx"])
    
    st.markdown("---")
    st.header("⚙️ E2E Economic Variables")
    if uploaded_file:
        sys_df = pd.read_excel(uploaded_file, sheet_name="System_Variables").set_index("Parameter")
        arr_rate = st.number_input("RM Arrival Rate (mins)", value=float(sys_df.loc["Arrival_Rate_Mins", "Value"]))
        rev_unit = st.number_input("Revenue/Unit (£)", value=float(sys_df.loc["Revenue_per_Unit", "Value"]))
        rm_cost = st.number_input("RM Cost/Unit (£)", value=float(sys_df.loc["RM_Cost_per_Unit", "Value"]))
        tax_rate = st.slider("Corporate Tax Rate", 0.0, 0.5, float(sys_df.loc["Tax_Rate", "Value"]))
        dso = st.number_input("Days Sales Outstanding (AR)", value=float(sys_df.loc["DSO_Days", "Value"]))
        dpo = st.number_input("Days Payable Outstanding (AP)", value=float(sys_df.loc["DPO_Days", "Value"]))
    else:
        arr_rate = st.number_input("RM Arrival Rate (mins)", value=5.0)
        rev_unit = st.number_input("Revenue/Unit (£)", value=500.0)
        rm_cost = st.number_input("RM Cost/Unit (£)", value=150.0)
        tax_rate = st.slider("Corporate Tax Rate", 0.0, 0.5, 0.25)
        dso = st.number_input("Days Sales Outstanding (AR)", value=45.0)
        dpo = st.number_input("Days Payable Outstanding (AP)", value=30.0)
        
    st.markdown("---")
    st.header("🎲 Simulation Fidelity")
    sim_time = 40 * 60 
    final_runs = st.slider("Verification Sample Size (Runs)", 10, 200, 50, step=10, help="Higher sample sizes shrink the Confidence Interval but take longer to compute.")
    
    run_analysis = st.button("🚀 Run AI Gap Analysis", type="primary", use_container_width=True)

# --- DYNAMIC STAGE EDITOR ---
st.subheader("🛠️ E2E Network Routing (Editable)")
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

edited_stages = st.data_editor(default_stages, num_rows="dynamic", use_container_width=True)
stage_names = edited_stages["Stage_Name"].tolist()
num_stages = len(stage_names)

# --- CORE DES EVALUATION FUNCTION (STATISTICAL VARIANCE) ---
def evaluate_network(qtys, speeds, num_runs=25, return_queues=False):
    mu_mods = [1.0 if s==0 else 0.7 for s in speeds]
    cap_mods = [1.0 if s==0 else 2.0 for s in speeds]
    op_mods = [1.0 if s==0 else 1.5 for s in speeds]
    
    total_capex = sum([qtys[i] * edited_stages.loc[i, "CAPEX_Base"] * cap_mods[i] for i in range(num_stages)])
    total_opex = sum([qtys[i] * edited_stages.loc[i, "OPEX_Weekly"] * op_mods[i] for i in range(num_stages)])
    annual_depr = total_capex / 10.0 
    
    run_roics, run_nopats, run_tps, run_wips = [], [], [], []
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

    def part_generator(env, resources):
        while True:
            yield env.timeout(random.expovariate(1.0 / arr_rate))
            env.process(part_journey(env, resources))

    def monitor_queues(env, resources, q_data, save_sample):
        while True:
            total_wip = sum([len(r.queue) for r in resources])
            wip_snapshot.append(total_wip)
            if save_sample:
                row = {"Time": env.now}
                for i in range(num_stages): row[f"{stage_names[i]} Queue"] = len(resources[i].queue)
                q_data.append(row)
            yield env.timeout(5)

    for r_idx in range(num_runs):
        env = simpy.Environment()
        resources = [simpy.Resource(env, capacity=qtys[i]) for i in range(num_stages)]
        mc_tp_run, wip_snapshot = [], []
        
        save_sample = return_queues and (r_idx == num_runs - 1)
        
        env.process(part_generator(env, resources))
        env.process(monitor_queues(env, resources, sample_q_data, save_sample))
        env.run(until=sim_time)
        
        # Calculate financials FOR THIS SPECIFIC RUN to capture variance
        tp_val = len(mc_tp_run)
        wip_val = np.mean(wip_snapshot) if wip_snapshot else 0
        
        wk_rev = tp_val * rev_unit
        wk_rm = tp_val * rm_cost
        wk_ebitda = wk_rev - wk_rm - total_opex
        
        ebit = (wk_ebitda * 52) - annual_depr
        nopat = ebit * (1 - tax_rate) if ebit > 0 else ebit
        
        ar_bal = ((wk_rev * 52) / 365.0) * dso
        ap_bal = ((wk_rm * 52) / 365.0) * dpo
        nwc = ar_bal + (wip_val * rm_cost) - ap_bal
        ic = total_capex + nwc
        
        roic_val = (nopat / ic) * 100 if ic > 0 else 0
        
        run_roics.append(roic_val)
        run_nopats.append(nopat / 52)
        run_tps.append(tp_val)
        run_wips.append(wip_val)

    # Statistical Aggregation (Mean & 95% Margin of Error)
    def calc_stats(data_array):
        mean_val = np.mean(data_array)
        std_val = np.std(data_array)
        moe_val = 1.96 * (std_val / np.sqrt(num_runs)) # 95% CI
        return mean_val, moe_val
        
    avg_roic, moe_roic = calc_stats(run_roics)
    avg_nopat, moe_nopat = calc_stats(run_nopats)
    avg_tp, moe_tp = calc_stats(run_tps)
    avg_wip, _ = calc_stats(run_wips)
    
    # Base financials off the mean for the dashboard deltas
    annual_rev = avg_tp * rev_unit * 52
    annual_rm = avg_tp * rm_cost * 52
    nwc_mean = ((annual_rev/365.0)*dso) + (avg_wip*rm_cost) - ((annual_rm/365.0)*dpo)
    
    q_avgs = []
    if return_queues and sample_q_data:
        df_q = pd.DataFrame(sample_q_data)
        for i in range(num_stages): q_avgs.append(df_q[f"{stage_names[i]} Queue"].mean())
    else:
        q_avgs = [0] * num_stages
    
    res = {
        "roic": avg_roic, "moe_roic": moe_roic,
        "nopat": avg_nopat, "moe_nopat": moe_nopat,
        "tp": avg_tp, "moe_tp": moe_tp,
        "wip": avg_wip, "capex": total_capex, "nwc": nwc_mean, "q_avgs": q_avgs,
        "opex": total_opex
    }
    if return_queues: return res, sample_q_data
    return res

# --- GRAPHVIZ VSM GENERATOR ---
def generate_vsm(qtys, speeds, metrics, view_type="ops"):
    mu_mods = [1.0 if s==0 else 0.7 for s in speeds]
    cap_mods = [1.0 if s==0 else 2.0 for s in speeds]
    op_mods = [1.0 if s==0 else 1.5 for s in speeds]
    q_avgs = metrics["q_avgs"]
    tp = max(1, metrics["tp"])
    
    dot = graphviz.Digraph(node_attr={'shape': 'box', 'style': 'filled', 'color': '#E1E4E8', 'fontname': 'Helvetica'})
    dot.attr(rankdir='LR', splines='ortho')
    
    if view_type == "ops":
        dot.node('IN', f'Raw Materials\nArrival: {arr_rate}m', color='#cce5ff', shape='folder')
    else:
        dot.node('IN', f'Raw Materials\n£{rm_cost:.2f} / unit', color='#cce5ff', shape='folder')
        
    prev_node = 'IN'
    total_cost_pu = rm_cost
    
    for i in range(num_stages):
        buffer_id = f'B{i}'
        
        if view_type == "ops":
            dot.node(buffer_id, f'WIP Buffer {i+1}\nAvg: {q_avgs[i]:.1f} parts', color='#fff3cd', shape='cylinder')
        else:
            wip_cap = q_avgs[i] * rm_cost
            dot.node(buffer_id, f'WIP Buffer {i+1}\nTrapped Cash:\n£{wip_cap:,.0f}', color='#f8d7da', shape='cylinder')
            
        dot.edge(prev_node, buffer_id)
        
        with dot.subgraph() as s:
            s.attr(rank='same')
            m_op_pu = (qtys[i] * edited_stages.loc[i, "OPEX_Weekly"] * op_mods[i]) / tp if qtys[i] > 0 else 0
            m_cap_pu = (((qtys[i] * edited_stages.loc[i, "CAPEX_Base"] * cap_mods[i]) / 520) / tp) if qtys[i] > 0 else 0
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
                
        merge_id = f'Merge{i}'
        dot.node(merge_id, '', shape='point', width='0')
        for m in range(qtys[i]): dot.edge(f'S{i}_M{m}', merge_id)
        prev_node = merge_id

    net_margin_pu = rev_unit - total_cost_pu
    if view_type == "ops":
        dot.node('OUT', f'Finished Goods\nThroughput: {metrics["tp"]:.0f}/wk\n(±{metrics["moe_tp"]:.0f})', color='#d4edda', shape='folder')
    else:
        dot.node('OUT', f'Finished Goods\nUnit Cost: £{total_cost_pu:.2f}\nNet Margin: £{net_margin_pu:.2f}', color='#d4edda', shape='folder')
        
    dot.edge(prev_node, 'OUT')
    return dot

# --- EXECUTION & ROIC HILL CLIMBER ---
if run_analysis:
    base_qtys = edited_stages["Qty_Machines"].tolist()
    base_speeds = [0] * num_stages 
    
    # The search phase uses a static 20 runs to keep UI snappy, the final verification uses the slider
    search_runs = 20 
    
    with st.spinner(f"Evaluating Client Baseline ({final_runs} Runs)..."):
        base_metrics_final, base_q_data = evaluate_network(base_qtys, base_speeds, num_runs=final_runs, return_queues=True)
        
    st_progress = st.empty()
    st_log = st.empty()
    
    curr_qtys = list(base_qtys)
    curr_speeds = list(base_speeds)
    
    # Get a fast baseline target for the search phase
    fast_base = evaluate_network(base_qtys, base_speeds, num_runs=search_runs)
    best_target = fast_base["roic"]
    
    search_log = ["🔍 **Commencing ROIC Search...**"]
    
    for step in range(5):
        st_progress.info(f"Optimization Step {step+1}: Evaluating ROIC across neighbors...")
        neighbors = []
        
        for i in range(num_stages):
            for delta in [-1, 1]:
                nq = list(curr_qtys)
                nq[i] += delta
                if 1 <= nq[i] <= 10: neighbors.append((nq, curr_speeds))
        for i in range(num_stages):
            ns = list(curr_speeds)
            ns[i] = 1 if curr_speeds[i] == 0 else 0
            neighbors.append((curr_qtys, ns))
            
        found_better = False
        for nq, ns in neighbors:
            m = evaluate_network(nq, ns, num_runs=search_runs)
            if m["roic"] > best_target:
                best_target = m["roic"]
                curr_qtys, curr_speeds = nq, ns
                found_better = True
                search_log.append(f"✅ Found higher ROIC state. Est. ROIC: {m['roic']:.1f}%")
                st_log.markdown("\n".join(search_log))
                
        if not found_better:
            search_log.append("🛑 Local Maxima Found. Terminating search.")
            st_log.markdown("\n".join(search_log))
            break
            
    st_progress.empty()
    st_log.empty()
    
    with st.spinner(f"Verifying Optimal Configuration ({final_runs} High-Fidelity Runs)..."):
        opt_metrics, opt_q_data = evaluate_network(curr_qtys, curr_speeds, num_runs=final_runs, return_queues=True)

    # --- UI DASHBOARDS ---
    t1, t2, t3, t4 = st.tabs(["🏆 Executive E2E Scorecard", "🗺️ Value Stream Maps", "📈 Queue Physics", "📥 CFO Audit Ledger"])

    with t1:
        st.subheader("Boardroom Gap Analysis: Baseline vs. Optimized")
        st.markdown(f"*All operational metrics represent the mean of **{final_runs} independent simulation runs**. The ± value indicates the 95% Confidence Interval.*")
        
        d_roic = opt_metrics["roic"] - base_metrics_final["roic"]
        d_nopat = opt_metrics["nopat"] - base_metrics_final["nopat"]
        d_cap = opt_metrics["capex"] - base_metrics_final["capex"]
        d_nwc = opt_metrics["nwc"] - base_metrics_final["nwc"]
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Annualized ROIC", f"{opt_metrics['roic']:.1f}% ±{opt_metrics['moe_roic']:.1f}%", f"{d_roic:.1f}% vs Base")
        c2.metric("Weekly NOPAT", f"£{opt_metrics['nopat']:,.0f} ±£{opt_metrics['moe_nopat']:,.0f}", f"£{d_nopat:,.0f} vs Base")
        c3.metric("Net Working Capital (NWC)", f"£{opt_metrics['nwc']:,.0f}", f"£{d_nwc:,.0f} vs Base", delta_color="inverse")
        c4.metric("Total CAPEX Deployed", f"£{opt_metrics['capex']:,.0f}", f"£{d_cap:,.0f} new spend", delta_color="inverse")

    with t2:
        st.markdown("Toggle between Physics (Flow) and Financial (Activity-Based Costing) views. The parallel routing visually demonstrates capacity width.")
        v_base, v_opt = st.tabs(["📊 Baseline State Diagrams", "🚀 Optimized State Diagrams"])
        
        with v_base:
            st.markdown("### Operations & Physics Flow")
            st.graphviz_chart(generate_vsm(base_qtys, base_speeds, base_metrics_final, "ops"), use_container_width=True)
            st.markdown("### Financial Unit Cost Waterfall")
            st.graphviz_chart(generate_vsm(base_qtys, base_speeds, base_metrics_final, "fin"), use_container_width=True)
            
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
            pd.DataFrame([base_metrics_final]).to_excel(writer, sheet_name="Base_Financials", index=False)
            pd.DataFrame([opt_metrics]).to_excel(writer, sheet_name="Opt_Financials", index=False)
            
        st.download_button("📥 Export Boardroom Audit (.xlsx)", data=output.getvalue(), file_name="E2E_Gap_Analysis.xlsx")

else:
    st.info("👈 Set the E2E economic parameters and hit 'Run AI Gap Analysis'.")
