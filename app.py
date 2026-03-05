import streamlit as st
import simpy
import random
import pandas as pd
import plotly.express as px
import numpy as np

st.set_page_config(page_title="DES Factory Twin", layout="wide")

st.title("🏭 Multi-Stage DES: Monte Carlo Twin")
st.markdown("""
This model simulates a 3-stage value stream (Milling $\\rightarrow$ Assembly $\\rightarrow$ QA). 
It runs **50 distinct simulations** to average out the chaos and find the true baseline, while plotting a sample timeline to visualize how variance migrates bottlenecks across the factory floor.
""")

# --- SIDEBAR CONTROLS ---
with st.sidebar:
    st.header("⚙️ System Inputs")
    arrival_rate = st.slider("Part Arrival Rate (mins)", 1.0, 15.0, 5.0, help="Average time between new parts arriving.")
    
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

    st.markdown("---")
    sim_time = st.slider("Hours per Simulation Run", 8, 168, 40) * 60 
    iterations = 50 # Monte Carlo runs
    run_sim = st.button("🎲 Run 50 Simulations", type="primary", use_container_width=True)

# --- DES ENGINE ---
if run_sim:
    
    # Global metrics across all 50 runs
    mc_lead_times = []
    mc_throughput = []
    
    # We will save the queue data from the *last* run to plot the visual chart
    sample_queue_data = []

    def part_journey(env, stages, metrics):
        """The routing logic for a single part."""
        arrival_time = env.now
        
        # 1. Milling
        with stages['Milling'].request() as req1:
            yield req1
            yield env.timeout(max(0.1, random.gauss(m1_mu, m1_sig)))
            
        # 2. Assembly
        with stages['Assembly'].request() as req2:
            yield req2
            yield env.timeout(max(0.1, random.gauss(m2_mu, m2_sig)))
            
        # 3. QA
        with stages['QA'].request() as req3:
            yield req3
            yield env.timeout(max(0.1, random.gauss(m3_mu, m3_sig)))
            
        # Record total lead time for this part
        metrics['lead_times'].append(env.now - arrival_time)

    def part_generator(env, stages, metrics):
        """Generates raw material over time."""
        while True:
            yield env.timeout(random.expovariate(1.0 / arrival_rate))
            env.process(part_journey(env, stages, metrics))

    def monitor_queues(env, stages, q_data):
        """Snapshots the Work-In-Progress (WIP) queues."""
        while True:
            q_data.append({
                "Time (Mins)": env.now,
                "Milling Queue": len(stages['Milling'].queue),
                "Assembly Queue": len(stages['Assembly'].queue),
                "QA Queue": len(stages['QA'].queue)
            })
            yield env.timeout(5) # Poll every 5 mins

    # --- MONTE CARLO LOOP ---
    progress_text = "Running Monte Carlo Simulations..."
    my_bar = st.progress(0, text=progress_text)
    
    for i in range(iterations):
        env = simpy.Environment()
        
        # Setup resources for this specific run
        stages = {
            'Milling': simpy.Resource(env, capacity=m1_qty),
            'Assembly': simpy.Resource(env, capacity=m2_qty),
            'QA': simpy.Resource(env, capacity=m3_qty)
        }
        
        run_metrics = {'lead_times': []}
        
        # Only collect the chart data on the final run to keep the UI clean
        run_q_data = []
        
        env.process(part_generator(env, stages, run_metrics))
        if i == iterations - 1:
            env.process(monitor_queues(env, stages, run_q_data))
            
        env.run(until=sim_time)
        
        # Aggregate run results
        mc_throughput.append(len(run_metrics['lead_times']))
        if run_metrics['lead_times']:
            mc_lead_times.extend(run_metrics['lead_times'])
            
        if i == iterations - 1:
            sample_queue_data = run_q_data
            
        my_bar.progress((i + 1) / iterations, text=progress_text)
        
    my_bar.empty()

    # --- UI RESULTS ---
    st.header("📊 Monte Carlo Results (Average of 50 Runs)")
    
    avg_throughput = np.mean(mc_throughput)
    avg_lead_time = np.mean(mc_lead_times) if mc_lead_times else 0
    p95_lead_time = np.percentile(mc_lead_times, 95) if mc_lead_times else 0
    
    c1, c2, c3 = st.columns(3)
    c1.metric("Avg Throughput per Shift", f"{avg_throughput:.0f} parts")
    c2.metric("Avg Lead Time (Arrival to Exit)", f"{avg_lead_time:.1f} mins")
    c3.metric("95th Percentile Lead Time", f"{p95_lead_time:.1f} mins", help="95% of parts finish faster than this time. High variance heavily impacts this metric.")
    
    st.markdown("---")
    st.subheader("Sample Timeline: Work-In-Progress (WIP) Queues")
    st.info("This plots the queues from one specific simulation run. Notice how a bottleneck in Milling starves Assembly, but when Milling clears, Assembly suddenly gets flooded and spikes.")
    
    df_q = pd.DataFrame(sample_queue_data)
    df_melt = df_q.melt(id_vars="Time (Mins)", var_name="Station", value_name="Parts in Queue")
    
    fig = px.area(df_melt, x="Time (Mins)", y="Parts in Queue", color="Station", 
                  title="Factory Bottleneck Migration",
                  color_discrete_map={"Milling Queue": "#1f77b4", "Assembly Queue": "#ff7f0e", "QA Queue": "#2ca02c"})
    st.plotly_chart(fig, use_container_width=True)

else:
    st.info("👈 Set your machine capacities, processing times, and variance, then run the Monte Carlo simulation.")
