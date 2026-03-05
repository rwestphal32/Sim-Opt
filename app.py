import streamlit as st
import simpy
import random
import pandas as pd
import plotly.express as px
import numpy as np
import graphviz
import time

st.set_page_config(page_title="DES Factory Twin", layout="wide")

st.title("🏭 Sim-Opt Engine: The Heuristic Twin")
st.markdown("This tool wraps a **Hill-Climbing Optimization Algorithm** around the Discrete Event Simulator. It automatically searches for the layout that maximizes **Weekly Net Profit**, balancing the cost of WIP queues against the depreciation of adding machines or upgrading to 'High-Speed' equipment.")

# --- SIDEBAR CONTROLS ---
with st.sidebar:
    st.header("⚙️ Baseline Physics")
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
    
    st.markdown("**Base CAPEX & OPEX (£)**")
    base_capex = [150000, 85000, 40000]
    base_opex = [2000, 3000, 1500]
    
    sim_time = 40 * 60 # 40 hours
    iterations = 50 

    st.markdown("---")
    run_manual = st.button("▶️ Run Manual Baseline", use_container_width=True)
    run_opt = st.button("🚀 Run AI Sim-Opt Search", type="primary", use_container_width=True)

# --- CORE DES EVALUATION FUNCTION ---
def evaluate_layout(q1, q2, q3, s1, s2, s3, num_runs=15):
    """Runs the DES for a specific machine configuration and returns Net Profit."""
    # Speed modifiers: s=0 (Normal), s=1 (High-Speed)
    mu_mods = [1.0 if s==0 else 0.7 for s in [s1, s2, s3]]
    cap_mods = [1.0 if s==0 else 2.0 for s in [s1, s2, s3]]
    op_mods = [1.0 if s==0 else 1.5 for s in [s1, s2, s3]]
    
    mc_tp = []
    wip_counts = []
    
    def part_journey(env, stages):
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

    def part_generator(env, stages):
        while True:
            yield env.timeout(random.expovariate(1.0 / arrival_rate))
            env.process(part_journey(env, stages))

    def monitor_queues(env, stages, q_data):
        while True:
            q_data.append(len(stages['M1'].queue) + len(stages['M2'].queue) + len(stages['M3'].queue))
            yield env.timeout(5)

    for i in range(num_runs):
        env = simpy.Environment()
        stages = {'M1': simpy.Resource(env, capacity=q1), 'M2': simpy.Resource(env, capacity=q2), 'M3': simpy.Resource(env, capacity=q3)}
        mc_tp_run = []
        run_wip = []
        
        env.process(part_generator(env, stages))
        env.process(monitor_queues(env, stages, run_wip))
        env.run(until=sim_time)
        
        mc_tp.append(len(mc_tp_run))
        wip_counts.append(np.mean(run_wip))

    avg_tp = np.mean(mc_tp)
    avg_wip = np.mean(wip_counts)
    
    # Financials
    total_capex = (q1*base_capex[0]*cap_mods[0]) + (q2*base_capex[1]*cap_mods[1]) + (q3*base_capex[2]*cap_mods[2])
    total_opex = (q1*base_opex[0]*op_mods[0]) + (q2*base_opex[1]*op_mods[1]) + (q3*base_opex[2]*op_mods[2])
    
    weekly_rev = avg_tp * unit_revenue
    weekly_rm = avg_tp * rm_cost
    weekly_wip_cost = avg_wip * wip_cost_per_unit
    weekly_depr = total_capex / 520.0 # 10-year straight line
    
    net_profit = weekly_rev - weekly_rm - total_opex - weekly_wip_cost - weekly_depr
    return net_profit, avg_tp, avg_wip, total_capex

# --- GRAPHVIZ GENERATOR ---
def generate_vsm(state, tp, wip, capex, profit):
    q1, q2, q3, s1, s2, s3 = state
    mu_mods = [1.0 if s==0 else 0.7 for s in [s1, s2, s3]]
    cap_mods = [1.0 if s==0 else 2.0 for s in [s1, s2, s3]]
    op_mods = [1.0 if s==0 else 1.5 for s in [s1, s2, s3]]
    
    dot = graphviz.Digraph(node_attr={'shape': 'box', 'style': 'filled', 'color': '#E1E4E8', 'fontname': 'Helvetica'})
    dot.attr(rankdir='LR', splines='ortho')
    
    tp = max(1, tp)
    dot.node('IN', f'Raw Materials\n£{rm_cost:.2f} / unit', color='#cce5ff', shape='folder')
    dot.node('OUT', f'Finished Goods\nThroughput: {tp:.0f}/wk\nNet Profit: £{profit:,.0f}/wk', color='#d4edda', shape='folder')
    
    # Simplified buffers for visual brevity
    dot.node('B', f'Total WIP Holding\nAvg {wip:.1f} parts\nCost: £{wip*wip_cost_per_unit:,.0f}/wk', color='#f8d7da', shape='cylinder')
    dot.edge('IN', 'B')
    
    with dot.subgraph() as sys:
        sys.attr(rank='same')
        
        m1_lbl = f'Milling x{q1}\n{"⚡ High-Speed" if s1 else "Standard"}\nμ={m1_mu*mu_mods[0]:.1f}m\nCAPEX: £{q1*base_capex[0]*cap_mods[0]/1000:.0f}k'
        sys.node('M1', m1_lbl, color='#e2e3e5' if s1==0 else '#ffeeba')
        
        m2_lbl = f'Assembly x{q2}\n{"⚡ High-Speed" if s2 else "Standard"}\nμ={m2_mu*mu_mods[1]:.1f}m\nCAPEX: £{q2*base_capex[1]*cap_mods[1]/1000:.0f}k'
        sys.node('M2', m2_lbl, color='#e2e3e5' if s2==0 else '#ffeeba')
        
        m3_lbl = f'QA x{q3}\n{"⚡ High-Speed" if s3 else "Standard"}\nμ={m3_mu*mu_mods[2]:.1f}m\nCAPEX: £{q3*base_capex[2]*cap_mods[2]/1000:.0f}k'
        sys.node('M3', m3_lbl, color='#e2e3e5' if s3==0 else '#ffeeba')
        
        dot.edge('B', 'M1'); dot.edge('M1', 'M2'); dot.edge('M2', 'M3'); dot.edge('M3', 'OUT')
        
    return dot

# --- HILL CLIMBING OPTIMIZER ---
if run_opt or run_manual:
    
    start_state = (m1_qty, m2_qty, m3_qty, 0, 0, 0) # Qty1, Qty2, Qty3, Speed1, Speed2, Speed3
    
    if run_manual:
        st.subheader("Manual Baseline Results")
        with st.spinner("Evaluating baseline..."):
            prof, tp, wip, cap = evaluate_layout(*start_state, num_runs=50)
            st.graphviz_chart(generate_vsm(start_state, tp, wip, cap, prof), use_container_width=True)
            st.stop()
            
    # --- AUTO-OPTIMIZATION RUN ---
    st.subheader("🤖 AI Sim-Opt Search (Hill Climbing)")
    st.markdown("Searching for the optimal balance of Capacity (Quantity) vs. Performance (High-Speed Upgrades).")
    
    search_log = []
    current_state = start_state
    best_profit, b_tp, b_wip, b_cap = evaluate_layout(*current_state, num_runs=10) # Fast eval
    
    st_progress = st.empty()
    st_log = st.empty()
    
    for step in range(5): # Max 5 climbing steps to prevent infinite loops
        st_progress.info(f"Step {step+1}: Evaluating neighbors of layout {current_state}...")
        
        neighbors = []
        # Generate +1/-1 Quantity Neighbors
        for i in range(3):
            for delta in [-1, 1]:
                n = list(current_state)
                n[i] += delta
                if 1 <= n[i] <= 5: neighbors.append(tuple(n))
        # Generate Speed Upgrade Neighbors
        for i in range(3, 6):
            n = list(current_state)
            n[i] = 1 if current_state[i] == 0 else 0
            neighbors.append(tuple(n))
            
        found_better = False
        
        for n in neighbors:
            p, t, w, c = evaluate_layout(*n, num_runs=10) # Fast eval for search
            if p > best_profit:
                best_profit = p
                current_state = n
                b_tp, b_wip, b_cap = t, w, c
                found_better = True
                search_log.append(f"✅ **Improved:** Moved to {n} | New Profit: £{p:,.0f}/wk")
                st_log.markdown("\n".join(search_log))
                
        if not found_better:
            search_log.append(f"🛑 **Local Maxima Found:** No neighbor improves profit. Stopping search.")
            st_log.markdown("\n".join(search_log))
            break
            
    st_progress.success("Optimization Complete!")
    
    st.markdown("---")
    st.header("🏆 Optimized Factory Blueprint")
    st.markdown("The algorithm has verified this layout with a high-fidelity 50-run Monte Carlo simulation.")
    
    with st.spinner("Running final high-fidelity verification..."):
        final_prof, final_tp, final_wip, final_cap = evaluate_layout(*current_state, num_runs=50)
        
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Optimized Net Profit", f"£{final_prof:,.0f}/wk")
    c2.metric("Total CAPEX Required", f"£{final_cap:,.0f}")
    c3.metric("Throughput", f"{final_tp:.0f} units/wk")
    c4.metric("Avg WIP Holding", f"{final_wip:.1f} units")
    
    st.graphviz_chart(generate_vsm(current_state, final_tp, final_wip, final_cap, final_prof), use_container_width=True)
    
    st.info("💡 **Notice the AI's Choice:** Did it buy 4 Standard machines, or did it choose to pay double CAPEX for a 'High-Speed' upgrade to smash a specific bottleneck? It calculates exactly which option generates a higher fully-burdened margin.")
