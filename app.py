import streamlit as st
import simpy
import random
import pandas as pd
import plotly.express as px
import numpy as np
import graphviz
import io

st.set_page_config(page_title="DES Factory Twin", layout="wide")

st.title("🏭 Sim-Opt Engine: The Consulting Gap Analysis")
st.markdown("This tool evaluates a client's **Baseline Configuration** and deploys a Hill-Climbing Heuristic to find the mathematically optimal state. It provides a boardroom-ready Gap Analysis proving the financial and operational ROI of changing the network.")

# --- SIDEBAR CONTROLS (THE CLIENT'S BASELINE) ---
with st.sidebar:
    st.header("⚙️ Client Baseline Physics")
    arrival_rate = st.slider("Part Arrival Rate (mins)", 1.0, 15.0, 5.0)
    
    st.markdown("---")
    m1_qty = st.number_input("Milling Machines", 1, 10, 2)
    m1_mu = st.slider("Milling Mean (mins)", 1.0, 20.0, 8.0)
    m1_sig = st.slider("Milling Std Dev (mins)", 0.0, 5.0, 1.0)
    
    st.markdown("---")
    m2_qty = st.number_input("Assembly Machines", 1, 10, 2)
    m2_mu = st.slider("Assembly Mean (mins)", 1.0, 20.0, 12.0)
    m2_sig = st.slider("Assembly Std Dev (mins)", 0.0, 5.0, 2.0)
    
    st.markdown("---")
    m3_qty = st.number_input("QA Machines", 1, 10, 1)
    m3_mu = st.slider("QA Mean (mins)", 1.0, 20.0, 4.0)
    m3_sig = st.slider("QA Std Dev (mins)", 0.0, 5.0, 0.5)

    st.header("💸 Financial Economics")
    unit_revenue = st.number_input("Revenue per Unit (£)", value=500)
    rm_cost = st.number_input("Raw Material Cost (£)", value=150)
    wip_cost_per_unit = st.number_input("WIP Penalty/Unit/Week (£)", value=15)
    
    base_capex = [150000, 85000, 40000]
    base_opex = [2000, 3000, 1500]
    
    sim_time = 40 * 60 # 40 hours
    
    st.markdown("---")
    run_analysis = st.button("🚀 Run Gap Analysis", type="primary", use_container_width=True)

# --- CORE DES EVALUATION FUNCTION ---
def evaluate_layout(state, num_runs=50, return_queues=False):
    q1, q2, q3, s1, s2, s3 = state
    mu_mods = [1.0 if s==0 else 0.7 for s in [s1, s2, s3]]
    cap_mods = [1.0 if s==0 else 2.0 for s in [s1, s2, s3]]
    op_mods = [1.0 if s==0 else 1.5 for s in [s1, s2, s3]]
    
    mc_tp = []
    mc_lt = []
    wip_counts = []
    sample_q_data = []

    def part_journey(env, stages):
        arr = env.now
        with stages['M1'].request() as req1:
            yield req1
            yield env.timeout(max(0.1, random.gauss(m1_mu * mu_mods[0], m1_sig)))
        with stages['M2'].request() as req2:
            yield req2
            yield env.timeout(max(0.1, random.gauss(m2_mu * mu_mods[1], m2_sig)))
        with stages['M3'].request() as req3:
            yield req3
            yield env.timeout(max(0.1, random.gauss(m3_mu * mu_mods[2], m3_sig)))
        mc_tp_run.append(1)
        mc_lt_run.append(env.now - arr)

    def part_generator(env, stages):
        while True:
            yield env.timeout(random.expovariate(1.0 / arrival_rate))
            env.process(part_journey(env, stages))

    def monitor_queues(env, stages, q_data, save_sample):
        while True:
            total_wip = len(stages['M1'].queue) + len(stages['M2'].queue) + len(stages['M3'].queue)
            run_wip.append(total_wip)
            if save_sample:
                q_data.append({
                    "Time": env.now, "Milling Queue": len(stages['M1'].queue),
                    "Assembly Queue": len(stages['M2'].queue), "QA Queue": len(stages['M3'].queue)
                })
            yield env.timeout(5)

    for i in range(num_runs):
        env = simpy.Environment()
        stages = {'M1': simpy.Resource(env, capacity=q1), 'M2': simpy.Resource(env, capacity=q2), 'M3': simpy.Resource(env, capacity=q3)}
        mc_tp_run, mc_lt_run, run_wip = [], [], []
        
        save_sample = return_queues and (i == num_runs - 1)
        
        env.process(part_generator(env, stages))
        env.process(monitor_queues(env, stages, sample_q_data, save_sample))
        env.run(until=sim_time)
        
        mc_tp.append(len(mc_tp_run))
        if mc_lt_run: mc_lt.extend(mc_lt_run)
        wip_counts.append(np.mean(run_wip))

    avg_tp = np.mean(mc_tp)
    avg_wip = np.mean(wip_counts)
    avg_lt = np.mean(mc_lt) if mc_lt else 0
    
    total_capex = (q1*base_capex[0]*cap_mods[0]) + (q2*base_capex[1]*cap_mods[1]) + (q3*base_capex[2]*cap_mods[2])
    total_opex = (q1*base_opex[0]*op_mods[0]) + (q2*base_opex[1]*op_mods[1]) + (q3*base_opex[2]*op_mods[2])
    
    weekly_rev = avg_tp * unit_revenue
    weekly_rm = avg_tp * rm_cost
    weekly_wip_cost = avg_wip * wip_cost_per_unit
    weekly_depr = total_capex / 520.0 
    
    net_profit = weekly_rev - weekly_rm - total_opex - weekly_wip_cost - weekly_depr
    
    res = {"profit": net_profit, "tp": avg_tp, "wip": avg_wip, "lt": avg_lt, "capex": total_capex, "opex": total_opex}
    if return_queues: return res, sample_q_data
    return res

# --- GRAPHVIZ VSM GENERATOR ---
def generate_vsm(state, metrics, title="Value Stream Map"):
    q1, q2, q3, s1, s2, s3 = state
    mu_mods = [1.0 if s==0 else 0.7 for s in [s1, s2, s3]]
    cap_mods = [1.0 if s==0 else 2.0 for s in [s1, s2, s3]]
    
    dot = graphviz.Digraph(node_attr={'shape': 'box', 'style': 'filled', 'color': '#E1E4E8', 'fontname': 'Helvetica'})
    dot.attr(rankdir='LR', splines='ortho', label=title, labelloc='t', fontsize='16')
    
    dot.node('IN', f'Raw Materials\nArrival: {arrival_rate}m', color='#cce5ff', shape='folder')
    dot.node('OUT', f'Finished Goods\nThroughput: {metrics["tp"]:.0f}/wk\nProfit: £{metrics["profit"]:,.0f}/wk', color='#d4edda', shape='folder')
    
    dot.node('B', f'Total WIP Holding\nAvg {metrics["wip"]:.1f} parts\nCost: £{metrics["wip"]*wip_cost_per_unit:,.0f}/wk', color='#f8d7da', shape='cylinder')
    dot.edge('IN', 'B')
    
    with dot.subgraph() as sys:
        sys.attr(rank='same')
        sys.node('M1', f'Milling x{q1}\n{"⚡ High-Speed" if s1 else "Standard"}\nμ={m1_mu*mu_mods[0]:.1f}m\nCAPEX: £{q1*base_capex[0]*cap_mods[0]/1000:.0f}k', color='#e2e3e5' if s1==0 else '#ffeeba')
        sys.node('M2', f'Assembly x{q2}\n{"⚡ High-Speed" if s2 else "Standard"}\nμ={m2_mu*mu_mods[1]:.1f}m\nCAPEX: £{q2*base_capex[1]*cap_mods[1]/1000:.0f}k', color='#e2e3e5' if s2==0 else '#ffeeba')
        sys.node('M3', f'QA x{q3}\n{"⚡ High-Speed" if s3 else "Standard"}\nμ={m3_mu*mu_mods[2]:.1f}m\nCAPEX: £{q3*base_capex[2]*cap_mods[2]/1000:.0f}k', color='#e2e3e5' if s3==0 else '#ffeeba')
        
        dot.edge('B', 'M1'); dot.edge('M1', 'M2'); dot.edge('M2', 'M3'); dot.edge('M3', 'OUT')
        
    return dot

# --- EXECUTION & HILL CLIMBER ---
if run_analysis:
    baseline_state = (m1_qty, m2_qty, m3_qty, 0, 0, 0)
    
    with st.spinner("Evaluating Client Baseline (50 Runs)..."):
        base_metrics, base_q_data = evaluate_layout(baseline_state, num_runs=50, return_queues=True)
        
    st_progress = st.empty()
    st_log = st.empty()
    
    current_state = baseline_state
    best_profit = base_metrics["profit"]
    search_log = ["🔍 **Commencing Heuristic Search...**"]
    
    # The Hill Climber
    for step in range(5):
        st_progress.info(f"Optimization Step {step+1}: Evaluating neighbors...")
        neighbors = []
        for i in range(3):
            for delta in [-1, 1]:
                n = list(current_state)
                n[i] += delta
                if 1 <= n[i] <= 5: neighbors.append(tuple(n))
        for i in range(3, 6):
            n = list(current_state)
            n[i] = 1 if current_state[i] == 0 else 0
            neighbors.append(tuple(n))
            
        found_better = False
        for n in neighbors:
            m = evaluate_layout(n, num_runs=10) # Fast eval
            if m["profit"] > best_profit:
                best_profit = m["profit"]
                current_state = n
                found_better = True
                search_log.append(f"✅ Found better state: {n} | Est. Profit: £{m['profit']:,.0f}")
                st_log.markdown("\n".join(search_log))
                
        if not found_better:
            search_log.append("🛑 Local Maxima Found. Terminating search.")
            st_log.markdown("\n".join(search_log))
            break
            
    st_progress.empty()
    st_log.empty()
    
    with st.spinner("Verifying Optimal Configuration (50 Runs)..."):
        opt_metrics, opt_q_data = evaluate_layout(current_state, num_runs=50, return_queues=True)

    # --- UI DASHBOARDS ---
    t1, t2, t3, t4 = st.tabs(["🏆 Executive Gap Analysis", "🗺️ Value Stream Maps", "📈 Queue Physics", "📥 CFO Audit Ledger"])

    with t1:
        st.subheader("Consulting Scorecard: Baseline vs. Optimized")
        
        # Calculate Deltas
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
        elif d_prof <= 0:
            st.info("The current baseline is already optimal for the given parameters. No changes recommended.")

    with t2:
        col1, col2 = st.columns(2)
        with col1:
            st.graphviz_chart(generate_vsm(baseline_state, base_metrics, "Current Baseline Configuration"), use_container_width=True)
        with col2:
            st.graphviz_chart(generate_vsm(current_state, opt_metrics, "Optimized Configuration"), use_container_width=True)

    with t3:
        st.subheader("Physics Verification: WIP Queues Over Time")
        st.markdown("Overlay of the physical bottleneck buildup (Sample Run). Notice how the optimized state stabilizes the chaotic queue spikes seen in the baseline.")
        
        df_base = pd.DataFrame(base_q_data).melt(id_vars="Time", var_name="Station", value_name="Parts in Queue")
        df_base["State"] = "Baseline"
        df_opt = pd.DataFrame(opt_q_data).melt(id_vars="Time", var_name="Station", value_name="Parts in Queue")
        df_opt["State"] = "Optimized"
        
        df_combined = pd.concat([df_base, df_opt])
        fig = px.line(df_combined, x="Time", y="Parts in Queue", color="Station", line_dash="State",
                      title="Bottleneck Migration: Baseline vs Optimized",
                      color_discrete_map={"Milling Queue": "#1f77b4", "Assembly Queue": "#ff7f0e", "QA Queue": "#2ca02c"})
        st.plotly_chart(fig, use_container_width=True)

    with t4:
        st.subheader("Export Analysis Data")
        # Build comparison ledger
        comp_data = [
            {"Metric": "Milling Configuration", "Baseline": f"{baseline_state[0]}x Standard", "Optimized": f"{current_state[0]}x {'High-Speed' if current_state[3] else 'Standard'}"},
            {"Metric": "Assembly Configuration", "Baseline": f"{baseline_state[1]}x Standard", "Optimized": f"{current_state[1]}x {'High-Speed' if current_state[4] else 'Standard'}"},
            {"Metric": "QA Configuration", "Baseline": f"{baseline_state[2]}x Standard", "Optimized": f"{current_state[2]}x {'High-Speed' if current_state[5] else 'Standard'}"},
            {"Metric": "Weekly Profit", "Baseline": base_metrics["profit"], "Optimized": opt_metrics["profit"]},
            {"Metric": "CAPEX", "Baseline": base_metrics["capex"], "Optimized": opt_metrics["capex"]},
            {"Metric": "Avg Lead Time (Mins)", "Baseline": base_metrics["lt"], "Optimized": opt_metrics["lt"]},
        ]
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            pd.DataFrame(comp_data).to_excel(writer, sheet_name="Gap_Analysis", index=False)
            pd.DataFrame(base_q_data).to_excel(writer, sheet_name="Baseline_Queue_Raw", index=False)
            pd.DataFrame(opt_q_data).to_excel(writer, sheet_name="Optimal_Queue_Raw", index=False)
            
        st.download_button("📥 Export Boardroom Audit (.xlsx)", data=output.getvalue(), file_name="Digital_Twin_Gap_Analysis.xlsx")

else:
    st.info("👈 Set the client's current baseline parameters and hit 'Run Gap Analysis'.")
