import streamlit as st
import simpy
import random
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="DES Factory Twin", layout="wide")

st.title("🏭 Discrete Event Simulation (DES): The Chaos Twin")
st.markdown("""
Unlike MILP, which tells you the *perfect* answer in a *perfect* world, DES tells you the *likely* outcome in a *chaotic* world.
Adjust the sliders to see how random variance creates massive bottlenecks, even when the "average" math says you have enough capacity.
""")

# --- SIDEBAR CONTROLS ---
with st.sidebar:
    st.header("⚙️ Factory Physics")
    st.markdown("Set the *average* times. The simulation will use exponential/normal distributions to inject randomness.")
    
    num_machines = st.slider("Number of Machines", 1, 5, 1)
    arrival_rate = st.slider("Avg Part Arrival Time (mins)", 1.0, 10.0, 5.0)
    process_rate = st.slider("Avg Processing Time (mins)", 1.0, 10.0, 4.5)
    
    st.markdown("---")
    sim_time = st.slider("Hours to Simulate", 8, 168, 40) * 60 # Convert to minutes
    run_sim = st.button("🎲 Run Simulation", type="primary", use_container_width=True)

# --- SIMPY DES ENGINE ---
if run_sim:
    # We will store the data here to visualize later
    queue_data = []
    wait_times = []
    
    def part_generator(env, machine, arrival_rate, process_rate):
        """Generates parts randomly over time."""
        part_id = 0
        while True:
            # Random wait before the next part arrives (Exponential distribution)
            yield env.timeout(random.expovariate(1.0 / arrival_rate))
            part_id += 1
            
            # Start the part processing logic in the background
            env.process(part_task(env, f"Part_{part_id}", machine, process_rate))

    def part_task(env, name, machine, process_rate):
        """The journey of a single part."""
        arrival_time = env.now
        
        # 1. Request a machine
        with machine.request() as req:
            yield req # Wait in queue until machine is free
            
            wait_time = env.now - arrival_time
            wait_times.append(wait_time)
            
            # 2. Process the part (Normal distribution, 1 min standard dev)
            actual_process_time = max(0.1, random.gauss(process_rate, 1.0))
            yield env.timeout(actual_process_time)
            
    def monitor_queue(env, machine):
        """Checks the queue length every 10 minutes to plot the chart."""
        while True:
            queue_data.append({"Time (Mins)": env.now, "Queue Length": len(machine.queue)})
            yield env.timeout(10) # Check every 10 simulation minutes

    # --- RUN THE SIMULATION ---
    # Setup the SimPy Environment
    env = simpy.Environment()
    
    # Define the Resource (The Machines)
    factory_machine = simpy.Resource(env, capacity=num_machines)
    
    # Add the processes to the environment
    env.process(part_generator(env, factory_machine, arrival_rate, process_rate))
    env.process(monitor_queue(env, factory_machine))
    
    # Run the clock!
    with st.spinner("Running 1,000s of events..."):
        env.run(until=sim_time)

    # --- RESULTS & VISUALIZATION ---
    st.header("📊 Simulation Results")
    
    # KPIs
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Parts Processed", len(wait_times))
    c2.metric("Average Wait Time", f"{sum(wait_times)/len(wait_times):.1f} mins" if wait_times else "0 mins")
    c3.metric("Max Queue Reached", max([d["Queue Length"] for d in queue_data]))
    
    st.markdown("### The Physics of Queues")
    st.info("Notice that even if Arrival Time > Processing Time (meaning you technically have enough capacity), the queue still spikes due to random variance colliding. This is why spreadsheets fail at operations planning.")
    
    # Queue Chart
    df_q = pd.DataFrame(queue_data)
    fig = px.line(df_q, x="Time (Mins)", y="Queue Length", title="Factory Work-In-Progress (WIP) Backlog Over Time",
                  labels={"Queue Length": "Parts Waiting in Queue"},
                  color_discrete_sequence=["#FF4B4B"])
    fig.update_layout(yaxis_title="Parts in Queue", xaxis_title="Simulation Minute")
    
    # Add fill under line for visual effect
    fig.update_traces(fill='tozeroy')
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("👈 Set your factory processing speeds and click 'Run Simulation'. Try setting Arrival Time to 5.0 and Process Time to 4.8 to see what happens when a system is running at 96% utilization!")
