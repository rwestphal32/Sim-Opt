import streamlit as st
import simpy
import random
import pandas as pd
import plotly.express as px
import numpy as np
import graphviz

st.set_page_config(page_title="DES Factory Twin", layout="wide")

st.title("🏭 Multi-Stage DES: Financial & Flow Twin")
st.markdown("This digital twin merges physical factory physics (queues, variance, throughput) with the C-Suite financial reality. The Economic view allocates all CAPEX, OPEX, and holding penalties down to the **Per-Unit Level**.")

# --- SIDEBAR CONTROLS ---
with st.sidebar:
    st.header("⚙️ Factory Physics")
    arrival_rate = st.slider("Part Arrival Rate (mins)", 1.0, 15.0, 5.0)
    
    st.markdown("---")
    st.subheader("1️⃣ Milling Station")
    m1_qty = st.number_input("Milling Machines", 1, 10, 2)
    m1_mu = st.slider("Milling Mean (mins)", 1.0, 20.0, 8.0)
    m1_sig = st.slider("Milling Std Dev (mins)", 0.0, 5.0, 1.0)
    
    st.markdown("---")
    st.subheader("2️⃣ Assembly Station")
    m2_qty = st.number_input("Assembly Machines", 1, 10, 3)
    m2_mu = st.slider("Assembly Mean (mins)", 1.0, 20.0, 12.0)
    m2_sig = st.slider("Assembly Std Dev (mins)", 0.0, 5.0, 2.0)
    
    st.markdown("---")
    st.subheader("3️⃣ QA & Packaging")
    m3_qty = st.number_input("QA Machines", 1, 10, 1)
    m3_mu = st.slider("QA Mean (mins)", 1.0, 20.0, 4.0)
    m3_sig = st.slider("QA Std Dev (mins)", 0.0, 5.0, 0.5)

    st.header("💸 Financial Economics")
    unit_revenue = st.number_input("Revenue per Unit (£)", value=500)
    rm_cost = st.number_input("Raw Material Cost (£)", value=150)
    wip_cost_per_unit = st.number_input("WIP Holding Cost/Unit/Week (£)", value=10)
    
    st.markdown("**CAPEX per Machine (£)**")
    capex_m1 = st.number_input("Milling CAPEX", value=150000)
    capex_m2 = st.number_input("Assembly CAPEX", value=85000)
    capex_m3 = st.number_input("QA CAPEX", value=40000)
    
    st.markdown("**Weekly OPEX per Machine (£)**")
    opex_m1 = st.number_input("Milling OPEX", value=2000)
    opex_m2 = st.number_input("Assembly OPEX", value=3000)
    opex_m3 = st.number_input("QA OPEX", value=1500)

    st.markdown("---")
    sim_time = st.slider("Hours per Simulation Run", 8, 168, 40) * 60 
    iterations = 50 
    run_sim = st.button("🎲 Run 50 Simulations", type="primary", use_container_width=True)

# --- DES ENGINE ---
if run_sim:
    mc_throughput = []
    mc_lead_times = []
    sample_queue_data = []
    q1_avgs, q2_avgs, q3_avgs = [], [], []

    def part_journey(env, stages, metrics):
        arrival_time = env.now
        
        with stages['Milling'].request() as req1:
            yield req1
            yield env.timeout(max(0.1, random.gauss(m1_mu, m1_sig)))
            
        with stages['Assembly'].request() as req2:
            yield req2
            yield env.timeout(max(0.1, random.gauss(m2_mu, m2_sig)))
            
        with stages['QA'].request() as req3:
            yield req3
            yield env.timeout(max(0.1, random.gauss(m3_mu, m3_sig)))
            
        metrics['lead_times'].append(env.now - arrival_time)

    def part_generator(env, stages, metrics):
        while True:
            yield env.timeout(random.expovariate(1.0 / arrival_rate))
            env.process(part_journey(env, stages, metrics))

    def monitor_queues(env, stages, q_data):
        while True:
            q_data.append({
                "Time (Mins)": env.now,
                "Milling Queue": len(stages['Milling'].queue),
                "Assembly Queue": len(stages['Assembly'].queue),
                "QA Queue": len(stages['QA'].queue)
            })
            yield env.timeout(5)

    progress_text = "Running Monte Carlo Simulations..."
    my_bar = st.progress(0, text=progress_text)
    
    for i in range(iterations):
        env = simpy.Environment()
        stages = {
            'Milling': simpy.Resource(env, capacity=m1_qty),
            'Assembly': simpy.Resource(env, capacity=m2_qty),
            'QA': simpy.Resource(env, capacity=m3_qty)
        }
        
        run_metrics = {'lead_times': []}
        run_q_data = []
        
        env.process(part_generator(env, stages, run_metrics))
        env.process(monitor_queues(env, stages, run_q_data))
            
        env.run(until=sim_time)
        
        mc_throughput.append(len(run_metrics['lead_times']))
        if run_metrics['lead_times']:
            mc_lead_times.extend(run_metrics['lead_times'])
            
        df_run_q = pd.DataFrame(run_q_data)
        q1_avgs.append(df_run_q["Milling Queue"].mean())
        q2_avgs.append(df_run_q["Assembly Queue"].mean())
        q3_avgs.append(df_run_q["QA Queue"].mean())
            
        if i == iterations - 1:
            sample_queue_data = run_q_data
            
        my_bar.progress((i + 1) / iterations, text=progress_text)
        
    my_bar.empty()

    # --- AGGREGATE METRICS ---
    avg_throughput = np.mean(mc_throughput)
    avg_lead_time = np.mean(mc_lead_times) if mc_lead_times else 0
    avg_q1, avg_q2, avg_q3 = np.mean(q1_avgs), np.mean(q2_avgs), np.mean(q3_avgs)
    grand_avg_wip = avg_q1 + avg_q2 + avg_q3

    # --- FINANCIAL MATH ---
    total_capex = (m1_qty * capex_m1) + (m2_qty * capex_m2) + (m3_qty * capex_m3)
    weekly_opex = (m1_qty * opex_m1) + (m2_qty * opex_m2) + (m3_qty * opex_m3)
    
    weekly_revenue = avg_throughput * unit_revenue
    weekly_rm_cost = avg_throughput * rm_cost
    weekly_wip_penalty = grand_avg_wip * wip_cost_per_unit
    
    weekly_gross_profit = weekly_revenue - weekly_rm_cost - weekly_opex - weekly_wip_penalty
    payback_period = (total_capex / (weekly_gross_profit * 52)) * 12 if weekly_gross_profit > 0 else float('inf')

    # --- GRAPHVIZ GENERATOR FUNCTIONS ---
    def generate_vsm(view_type="ops"):
        dot = graphviz.Digraph(node_attr={'shape': 'box', 'style': 'filled', 'color': '#E1E4E8', 'fontname': 'Helvetica'})
        dot.attr(rankdir='LR', splines='ortho')
        
        # ACTIVITY-BASED COSTING MATH
        # Guard against zero division if nothing processes
        tp = max(1, avg_throughput) 
        
        # Buffer Penalties per Unit
        b1_pu = (avg_q1 * wip_cost_per_unit) / tp
        b2_pu = (avg_q2 * wip_cost_per_unit) / tp
        b3_pu = (avg_q3 * wip_cost_per_unit) / tp
        
        # Station Costs per Unit (Allocated evenly across parallel machines)
        # Amortizing CAPEX over 520 weeks (10 years)
        m1_op_pu = (m1_qty * opex_m1) / tp if m1_qty > 0 else 0
        m1_cap_pu = ((m1_qty * capex_m1) / 520) / tp if m1_qty > 0 else 0
        
        m2_op_pu = (m2_qty * opex_m2) / tp if m2_qty > 0 else 0
        m2_cap_pu = ((m2_qty * capex_m2) / 520) / tp if m2_qty > 0 else 0
        
        m3_op_pu = (m3_qty * opex_m3) / tp if m3_qty > 0 else 0
        m3_cap_pu = ((m3_qty * capex_m3) / 520) / tp if m3_qty > 0 else 0
        
        total_cost_pu = rm_cost + b1_pu + b2_pu + b3_pu + m1_op_pu + m1_cap_pu + m2_op_pu + m2_cap_pu + m3_op_pu + m3_cap_pu
        net_margin_pu = unit_revenue - total_cost_pu

        if view_type == "ops":
            dot.node('IN', f'Raw Materials\nArrival: {arrival_rate}m', color='#cce5ff', shape='folder')
            dot.node('B1', f'WIP Buffer 1\nAvg: {avg_q1:.1f} parts', color='#fff3cd', shape='cylinder')
            dot.node('B2', f'WIP Buffer 2\nAvg: {avg_q2:.1f} parts', color='#fff3cd', shape='cylinder')
            dot.node('B3', f'WIP Buffer 3\nAvg: {avg_q3:.1f} parts', color='#fff3cd', shape='cylinder')
            dot.node('OUT', f'Finished Goods\nThroughput: {avg_throughput:.0f}/wk\nLead Time: {avg_lead_time:.1f}m', color='#d4edda', shape='folder')
        else:
            dot.node('IN', f'Raw Materials\n£{rm_cost:.2f} / unit', color='#cce5ff', shape='folder')
            dot.node('B1', f'WIP Buffer 1\nPenalty: £{b1_pu:.2f} / unit', color='#f8d7da', shape='cylinder')
            dot.node('B2', f'WIP Buffer 2\nPenalty: £{b2_pu:.2f} / unit', color='#f8d7da', shape='cylinder')
            dot.node('B3', f'WIP Buffer 3\nPenalty: £{b3_pu:.2f} / unit', color='#f8d7da', shape='cylinder')
            dot.node('OUT', f'Finished Goods\nCost: £{total_cost_pu:.2f} / unit\nMargin: £{net_margin_pu:.2f} / unit', color='#d4edda', shape='folder')

        dot.edge('IN', 'B1')
        
        with dot.subgraph() as s1:
            s1.attr(rank='same')
            for i in range(m1_qty):
                label = f'Milling {i+1}\nμ={m1_mu}m, σ={m1_sig}m' if view_type == "ops" else f'Milling {i+1}\nOPEX: £{m1_op_pu/m1_qty:.2f} / unit\nDepr: £{m1_cap_pu/m1_qty:.2f} / unit'
                s1.node(f'M1_{i}', label)
                dot.edge('B1', f'M1_{i}')
                dot.edge(f'M1_{i}', 'B2')

        with dot.subgraph() as s2:
            s2.attr(rank='same')
            for i in range(m2_qty):
                label = f'Assembly {i+1}\nμ={m2_mu}m, σ={m2_sig}m' if view_type == "ops" else f'Assembly {i+1}\nOPEX: £{m2_op_pu/m2_qty:.2f} / unit\nDepr: £{m2_cap_pu/m2_qty:.2f} / unit'
                s2.node(f'M2_{i}', label)
                dot.edge('B2', f'M2_{i}')
                dot.edge(f'M2_{i}', 'B3')

        with dot.subgraph() as s3:
            s3.attr(rank='same')
            for i in range(m3_qty):
                label = f'QA {i+1}\nμ={m3_mu}m, σ={m3_sig}m' if view_type == "ops" else f'QA {i+1}\nOPEX: £{m3_op_pu/m3_qty:.2f} / unit\nDepr: £{m3_cap_pu/m3_qty:.2f} / unit'
                s3.node(f'M3_{i}', label)
                dot.edge('B3', f'M3_{i}')
                dot.edge(f'M3_{i}', 'OUT')

        return dot

    # --- UI RESULTS ---
    t1, t2, t3 = st.tabs(["🗺️ Dynamic Flowcharts", "💰 Financial Dashboard", "📊 Operations & Queues"])

    with t1:
        v_tab1, v_tab2 = st.tabs(["⚙️ Operations & Physics View", "💸 Financial Waterfall View"])
        
        with v_tab1:
            st.markdown("**Physical Layout:** Displays parallel routing, station speeds, variance, and the resulting physical WIP buildup.")
            st.graphviz_chart(generate_vsm("ops"), use_container_width=True)
            
        with v_tab2:
            st.markdown("**Economic Waterfall:** Displays Activity-Based Costing. As a unit moves left to right, it accumulates Raw Material costs, WIP queue penalties, OPEX, and Depreciation to calculate the true Landed Cost.")
            st.graphviz_chart(generate_vsm("fin"), use_container_width=True)

    with t2:
        st.subheader("Asset Return & Financial Economics")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Deployed CAPEX", f"£{total_capex:,.0f}")
        c2.metric("Weekly Revenue", f"£{weekly_revenue:,.0f}")
        c3.metric("Weekly Gross Margin", f"£{weekly_gross_profit:,.0f}")
        c4.metric("CAPEX Payback Period", f"{payback_period:.1f} Months" if payback_period != float('inf') else "Never (Loss)")
        
        st.markdown("### Weekly P&L (Based on Average Simulated Throughput)")
        pl_df = pd.DataFrame([
            {"Line Item": "Gross Revenue", "Amount (£)": weekly_revenue},
            {"Line Item": "Raw Material COGS", "Amount (£)": -weekly_rm_cost},
            {"Line Item": "Total Station OPEX", "Amount (£)": -weekly_opex},
            {"Line Item": "WIP Holding Penalty (Queue Cost)", "Amount (£)": -weekly_wip_penalty},
            {"Line Item": "Net Weekly Profit", "Amount (£)": weekly_gross_profit}
        ])
        st.dataframe(pl_df.style.format({"Amount (£)": "£{:,.2f}"}), use_container_width=True)

    with t3:
        st.subheader("Physics of the Factory Floor")
        c1, c2, c3 = st.columns(3)
        c1.metric("Avg Simulated Throughput", f"{avg_throughput:.0f} parts")
        c2.metric("Avg Lead Time", f"{avg_lead_time:.1f} mins")
        c3.metric("Avg Work-In-Progress (WIP)", f"{grand_avg_wip:.1f} parts waiting")
        
        st.markdown("### Work-In-Progress (WIP) Timeline")
        st.caption("Visualizing the queue spikes from a single sample run. These physical bottlenecks generate the 'WIP Holding Penalty' in the Financial tab.")
        
        df_q = pd.DataFrame(sample_queue_data)
        df_melt = df_q.melt(id_vars="Time (Mins)", var_name="Station", value_name="Parts in Queue")
        
        fig = px.area(df_melt, x="Time (Mins)", y="Parts in Queue", color="Station", 
                      color_discrete_map={"Milling Queue": "#1f77b4", "Assembly Queue": "#ff7f0e", "QA Queue": "#2ca02c"})
        st.plotly_chart(fig, use_container_width=True)

else:
    st.info("👈 Configure your layout and economics, then run the simulation to generate the dynamic flowcharts and P&L.")
