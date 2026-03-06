import streamlit as st
import simpy
import random
import pandas as pd
import plotly.express as px
import graphviz

st.set_page_config(page_title="DAG Factory Twin", layout="wide")

st.title("🏭 Universal DAG Engine: BOMs & Batching")
st.markdown("Build complex, multi-component value streams. Use the **Edges (BOM)** table to define exactly how many units a downstream machine pulls from an upstream buffer. The engine automatically handles assembly synchronization and batch accumulation.")

# --- DYNAMIC DAG CONFIGURATION ---
st.sidebar.header("⏱️ Simulation Settings")
sim_time = st.sidebar.slider("Simulation Hours", 8, 168, 40) * 60

st.subheader("1️⃣ Factory Nodes (Stations & Sources)")
st.markdown("Define your machines and raw material sources. Sources generate parts infinitely. Sinks collect finished goods.")

default_nodes = pd.DataFrame({
    "Node_ID": ["Raw_Metal", "Raw_Plastic", "Milling", "Molding", "Assembly", "Shipping", "Finished_Goods"],
    "Type": ["Source", "Source", "Machine", "Machine", "Machine", "Machine", "Sink"],
    "Machines_Qty": [1, 1, 2, 3, 2, 1, 1],
    "Mean_Mins": [5.0, 3.0, 8.0, 6.0, 12.0, 20.0, 0.0],
    "StdDev_Mins": [0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 0.0],
    "Yield_Qty": [1, 1, 1, 1, 1, 50, 0] # How many units this machine outputs per cycle (e.g., Shipping yields 50)
})

edited_nodes = st.data_editor(default_nodes, num_rows="dynamic", use_container_width=True)

st.subheader("2️⃣ Factory Edges (Bill of Materials & Routing)")
st.markdown("Define the flow. The **Qty_Required** column is the BOM or Batch size. (e.g., Assembly pulls 1 Metal and 2 Plastics. Shipping pulls 50 Assembled units).")

default_edges = pd.DataFrame({
    "From_Node": ["Raw_Metal", "Raw_Plastic", "Milling", "Molding", "Assembly", "Shipping"],
    "To_Node": ["Milling", "Molding", "Assembly", "Assembly", "Shipping", "Finished_Goods"],
    "Qty_Required": [1, 1, 1, 2, 50, 50] # E.g., Assembly needs 1 Milled, 2 Molded. Shipping needs 50 Assembled.
})

edited_edges = st.data_editor(default_edges, num_rows="dynamic", use_container_width=True)

run_sim = st.button("🚀 Run Universal DAG Simulation", type="primary", use_container_width=True)

# --- THE UNIVERSAL DAG SIMPY ENGINE ---
if run_sim:
    env = simpy.Environment()
    
    # Dictionaries to hold our SimPy objects dynamically
    buffers = {}
    machine_resources = {}
    
    # 1. Initialize Buffers (Containers) for every node's output
    for _, row in edited_nodes.iterrows():
        node_id = row["Node_ID"]
        buffers[node_id] = simpy.Container(env, init=0)
        if row["Type"] == "Machine":
            machine_resources[node_id] = simpy.Resource(env, capacity=int(row["Machines_Qty"]))

    # 2. Universal Process Logic
    def universal_node_process(env, node_id, node_type, qty, mean_t, std_t, yield_qty):
        # Find all inputs required for this node from the Edges table
        inputs = []
        for _, edge in edited_edges.iterrows():
            if edge["To_Node"] == node_id:
                inputs.append((edge["From_Node"], int(edge["Qty_Required"])))

        while True:
            # SOURCE LOGIC (Infinite Generator)
            if node_type == "Source":
                yield env.timeout(max(0.1, random.gauss(mean_t, std_t)))
                buffers[node_id].put(yield_qty)
                
            # MACHINE/ASSEMBLY/BATCH LOGIC
            elif node_type == "Machine":
                # Step A: Wait for ALL required components to be available in upstream buffers (BOM Logic)
                if inputs:
                    # env.all_of ensures the machine doesn't start until ALL components are ready
                    get_events = [buffers[in_node].get(req_qty) for in_node, req_qty in inputs]
                    yield env.all_of(get_events)
                
                # Step B: Request the physical machine resource
                with machine_resources[node_id].request() as req:
                    yield req
                    # Step C: Process (Time delay)
                    yield env.timeout(max(0.1, random.gauss(mean_t, std_t)))
                    
                # Step D: Output finished batch to its own downstream buffer
                buffers[node_id].put(yield_qty)
                
            # SINK LOGIC (End of the line, just stops)
            elif node_type == "Sink":
                yield env.timeout(100) # Sinks don't actively process, they just accumulate
                break

    # 3. Spin up all processes
    for _, row in edited_nodes.iterrows():
        env.process(universal_node_process(
            env=env,
            node_id=row["Node_ID"],
            node_type=row["Type"],
            qty=row["Machines_Qty"],
            mean_t=row["Mean_Mins"],
            std_t=row["StdDev_Mins"],
            yield_qty=row["Yield_Qty"]
        ))

    # 4. Monitor WIP Levels
    queue_data = []
    def monitor_network(env):
        while True:
            row = {"Time (Mins)": env.now}
            for n in buffers.keys():
                row[f"{n}_WIP"] = buffers[n].level
            queue_data.append(row)
            yield env.timeout(5)
            
    env.process(monitor_network(env))

    # 5. Run it!
    with st.spinner("Simulating E2E DAG Network..."):
        env.run(until=sim_time)
        
    df_q = pd.DataFrame(queue_data)

    # --- GRAPHVIZ DAG GENERATOR ---
    def generate_dag_vsm():
        dot = graphviz.Digraph(node_attr={'shape': 'box', 'style': 'filled', 'color': '#E1E4E8', 'fontname': 'Helvetica'})
        dot.attr(rankdir='LR')
        
        # Draw Nodes
        for _, row in edited_nodes.iterrows():
            nid = row["Node_ID"]
            ntype = row["Type"]
            final_wip = df_q[f"{nid}_WIP"].iloc[-1]
            
            if ntype == "Source":
                dot.node(nid, f'{nid}\n(Source)\nGen: 1 per ~{row["Mean_Mins"]}m\nBuffer WIP: {final_wip}', color='#cce5ff', shape='folder')
            elif ntype == "Sink":
                dot.node(nid, f'{nid}\n(Sink)\nTotal Finished Goods: {final_wip}', color='#d4edda', shape='folder')
            else:
                lbl = f'{nid}\n[{row["Machines_Qty"]}x Mach]\nμ={row["Mean_Mins"]}m, σ={row["StdDev_Mins"]}m\nYield: {row["Yield_Qty"]}\nBuffer WIP: {final_wip}'
                dot.node(nid, lbl, color='#e2e3e5')
                
        # Draw Edges (with BOM/Batch Qty labels)
        for _, edge in edited_edges.iterrows():
            dot.edge(edge["From_Node"], edge["To_Node"], label=f' Pull: {edge["Qty_Required"]}', fontsize='10', color='#0056b3', fontcolor='#0056b3')
            
        return dot

    # --- DASHBOARDS ---
    t1, t2 = st.tabs(["🗺️ E2E Network DAG", "📈 Work-In-Progress Physics"])
    
    with t1:
        st.subheader("Dynamic Bill of Materials & Routing")
        st.markdown("The numbers on the arrows indicate the **Pull Quantity**. Notice how the Assembly node joins two parallel streams based on an exact recipe, and the Shipping node pulls massive batches at once.")
        st.graphviz_chart(generate_dag_vsm(), use_container_width=True)
        
    with t2:
        st.subheader("WIP Buffer Accumulation Over Time")
        st.markdown("This chart visualizes where inventory is getting trapped. If `Raw_Metal_WIP` skyrockets but `Raw_Plastic_WIP` is zero, it means Assembly is starved for Plastic and the Metal is uselessly piling up.")
        
        # Melt the dataframe to plot all WIP levels easily
        df_melt = df_q.melt(id_vars="Time (Mins)", var_name="Buffer", value_name="Inventory Level")
        
        # Filter out the Sink so it doesn't skew the Y-axis (since Finished Goods just goes up forever)
        df_melt_wip = df_melt[~df_melt["Buffer"].str.contains("Finished_Goods")]
        
        fig = px.line(df_melt_wip, x="Time (Mins)", y="Inventory Level", color="Buffer", title="Network Bottleneck Migration")
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Finished Goods Accumulation")
        sink_nodes = edited_nodes[edited_nodes["Type"] == "Sink"]["Node_ID"].tolist()
        if sink_nodes:
            fg_col = f"{sink_nodes[0]}_WIP"
            fig_fg = px.area(df_q, x="Time (Mins)", y=fg_col, title="Cumulative Yield", color_discrete_sequence=['#2ca02c'])
            st.plotly_chart(fig_fg, use_container_width=True)

else:
    st.info("👈 Add/Edit your Nodes and Edges, then hit Run.")
