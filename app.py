import streamlit as st
import simpy
import random
import pandas as pd
import plotly.express as px
import numpy as np
import graphviz

st.set_page_config(page_title="DES Factory Twin", layout="wide")

st.title("🏭 Multi-Stage DES: Financial & Flow Twin")
st.markdown("This digital twin merges physical factory physics (queues, variance, throughput) with the C-Suite financial reality (CAPEX, OPEX, and the hidden cost of holding Work-in-Progress inventory).")

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
    wip_cost_per_unit = st.number_input("WIP Holding Cost/Unit/Week (£)", value=10, help="The cost of cash tied up in physical inventory sitting in queues.")
    
    st.markdown("**CAPEX per Machine (£)**")
    capex_m1 = st.number_input("Milling CAPEX", value=150000)
    capex_m2 = st.number_input("Assembly CAPEX", value=85000)
    capex_m3 = st.number_input("QA CAPEX", value=40000)
    
    st.markdown("**Weekly OPEX per Machine (£)**")
    opex_m1 = st.number_input("Milling OPEX (Labor/Power)", value=2000)
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
    avg_wip_counts = []

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
            
        # Calculate Average WIP for this run
        df_run_q = pd.DataFrame(run_q_data)
        avg_wip_run = df_run_q[["Milling Queue", "Assembly Queue", "QA Queue"]].sum(axis=1).mean()
        avg_wip_counts.append(avg_wip_run)
            
        if i == iterations - 1:
            sample_queue_data = run_q_data
            
        my_bar.progress((i + 1) / iterations, text=progress_text)
        
    my_bar.empty()

    # --- AGGREGATE METRICS ---
    avg_throughput = np.mean(mc_throughput)
    avg_lead_time = np.mean(mc_lead_times) if mc_lead_times else 0
    p95_lead_time = np.percentile(mc_lead_times, 95) if mc_lead_times else 0
    grand_avg_wip = np.mean(avg_wip_counts)

    # --- FINANCIAL MATH ---
    total_capex = (m1_qty * capex_m1) + (m2_qty * capex_m2) + (m3_qty * capex_m3)
    weekly_opex = (m1_qty * opex_m1) + (m2_qty * opex_m2) + (m3_qty * opex_m3)
    
    weekly_revenue = avg_throughput * unit_revenue
    weekly_rm_cost = avg_throughput * rm_cost
    weekly_wip_penalty = grand_avg_wip * wip_cost_per_unit
    
    weekly_gross_profit = weekly_revenue - weekly_rm_cost - weekly_opex - weekly_wip_penalty
    annual_gross_profit = weekly_gross_profit * 52
    payback_period = (total_capex / annual_gross_profit) * 12 if annual_gross_profit > 0 else float('inf')

    # --- UI RESULTS ---
    t1, t2, t3 = st.tabs(["🗺️ Value Stream Map", "💰 Financial Dashboard", "📊 Operations & Queues"])

    with t1:
        st.subheader("Dynamic Value Stream Flowchart")
        st.markdown("Visualizing the physical layout, capacities, and variance nodes.")
        
        # Build the Graphviz Chart dynamically
        dot = graphviz.Digraph(node_attr={'shape': 'record', 'style': 'filled', 'color': '#E1E4E8', 'fontname': 'Helvetica'})
        dot.attr(rankdir='LR') # Left to Right flow
        
        dot.node('In', f'Raw Materials\nArrival Rate: {arrival_rate}m', color='#cce5ff')
        dot.node('M1', f'Milling Station\n[{m1_qty}x Machines]\nμ={m1_mu}m, σ={m1_sig}m')
        dot.node('M2', f'Assembly Station\n[{m2_qty}x Machines]\nμ={m2_mu}m, σ={m2_sig}m')
        dot.node('M3', f'QA & Packaging\n[{m3_qty}x Machines]\nμ={m3_mu}m, σ={m3_sig}m')
        dot.node('Out', f'Finished Goods\nAvg Throughput: {avg_throughput:.0f} units', color='#d4edda')
        
        dot.edge('In', 'M1')
        dot.edge('M1', 'M2')
        dot.edge('M2', 'M3')
        dot.edge('M3', 'Out')
        
        st.graphviz_chart(dot, use_container_width=True)
        
        st.info("💡 **Operations Insight:** Look at the standard deviations (σ) in the boxes above. A high standard deviation in Milling will sequentially starve and flood Assembly, destroying the overall throughput.")

    with t2:
        st.subheader("Asset Return & Financial Economics")
        
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Deployed CAPEX", f"£{total_capex:,.0f}")
        c2.metric("Weekly Revenue", f"£{weekly_revenue:,.0f}")
        c3.metric("Weekly Gross Margin", f"£{weekly_gross_profit:,.0f}")
        c4.metric("CAPEX Payback Period", f"{payback_period:.1f} Months" if payback_period != float('inf') else "Never (Loss)")
        
        st.markdown("### Weekly P&L (Based on Simulation Throughput)")
        pl_df = pd.DataFrame([
            {"Line Item": "Gross Revenue", "Amount (£)": weekly_revenue},
            {"Line Item": "Raw Material COGS", "Amount (£)": -weekly_rm_cost},
            {"Line Item": "Station OPEX (Labor/Power)", "Amount (£)": -weekly_opex},
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
        st.caption("Visualizing the queue spikes from a single sample run. These bottlenecks generate the 'WIP Holding Penalty' in the Financial tab.")
        
        df_q = pd.DataFrame(sample_queue_data)
        df_melt = df_q.melt(id_vars="Time (Mins)", var_name="Station", value_name="Parts in Queue")
        
        fig = px.area(df_melt, x="Time (Mins)", y="Parts in Queue", color="Station", 
                      color_discrete_map={"Milling Queue": "#1f77b4", "Assembly Queue": "#ff7f0e", "QA Queue": "#2ca02c"})
        st.plotly_chart(fig, use_container_width=True)

else:
    st.info("👈 Configure your layout and economics, then run the simulation to generate the VSM and P&L.")
