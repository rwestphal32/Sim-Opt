import streamlit as st
import simpy
import random
import pandas as pd
import plotly.express as px
import graphviz

st.set_page_config(page_title="DAG Factory Twin", layout="wide")

st.title("🏭 Universal DAG Engine: BOMs & Parallel Flow")
st.markdown("The Sink physics have been repaired so inventory actually reaches Finished Goods. The dynamic visualizer has been rebuilt to show true **Parallel Machine Capacity** and intermediate WIP buffers.")

# --- DYNAMIC DAG CONFIGURATION ---
st.sidebar.header("⏱️ Simulation Settings")
sim_time = st.sidebar.slider("Simulation Hours", 8, 168, 40) * 60

st.subheader("1️⃣ Factory Nodes (Stations & Sources)")
default_nodes = pd.DataFrame({
    "Node_ID": ["Raw_Metal", "Raw_Plastic", "Milling", "Molding", "Assembly", "Shipping", "Finished_Goods"],
    "Type": ["Source", "Source", "Machine", "Machine", "Machine", "Machine", "Sink"],
    "Machines_Qty": [1, 1, 2, 3, 2, 1, 1],
    "Mean_Mins": [5.0, 3.0, 8.0, 6.0, 12.0, 20.0, 0.0],
    "StdDev_Mins": [0.0, 0.0, 1.0, 1.0, 2.0, 2.0, 0.0],
    "Yield_Qty": [1, 1, 1, 1, 1, 50, 50] # Sink now needs a yield qty to know how much to count!
})

edited_nodes = st.data_editor(default_nodes, num_rows="dynamic", use_container_width=True)

st.subheader("2️⃣ Factory Edges (Bill of Materials & Routing)")
default_edges = pd.DataFrame({
    "From_Node": ["Raw_Metal", "Raw_Plastic", "Milling", "Molding", "Assembly", "Shipping"],
    "To_Node": ["Milling", "Molding", "Assembly", "Assembly", "Shipping", "Finished_Goods"],
    "Qty_Required": [1, 1, 1, 2, 50, 50] 
})

edited_edges = st.data_editor(default_edges, num_rows="dynamic", use_container_width=True)

run_sim = st.button("🚀 Run Universal DAG Simulation", type="primary", use_container_width=True)

# --- THE UNIVERSAL DAG SIMPY ENGINE ---
if run_sim:
    env = simpy.Environment()
    buffers = {}
    machine_resources = {}
    
    # 1. Initialize Buffers (Containers)
    for _, row in edited_nodes.iterrows():
        node_id = row["Node_ID"]
        buffers[node_id] = simpy.Container(env, init=0)
        if row["Type"] == "Machine":
            machine_resources[node_id] = simpy.Resource(env, capacity=int(row["Machines_Qty"]))

    # 2. Universal Process Logic
    def universal_node_process(env, node_id, node_type, qty, mean_t, std_t, yield_qty):
        inputs = []
        for _, edge in edited_edges.iterrows():
            if edge["To_Node"] == node_id:
                inputs.append((edge["From_Node"], int(edge["Qty_Required"])))

        while True:
            if node_type == "Source":
                yield env.timeout(max(0.1, random.gauss(mean_t, std_t)))
                buffers[node_id].put(yield_qty)
                
            elif node_type == "Machine":
                if inputs:
                    get_events = [buffers[in_node].get(req_qty) for in_node, req_qty in inputs]
                    yield env.all_of(get_events)
                
                with machine_resources[node_id].request() as req:
                    yield req
                    yield env.timeout(max(0.1, random.gauss(mean_t, std_t)))
                    
                buffers[node_id].put(yield_qty)
                
            elif node_type == "Sink":
                # BUG FIX: Sinks must actively consume their required inputs to clear upstream buffers
                if inputs:
                    get_events = [buffers[in_node].get(req_qty) for in_node, req_qty in inputs]
                    yield env.all_of(get_events)
                    buffers[node_id].put(yield_qty) # Log the received goods
                    yield env.timeout(0) 
                else:
                    break # If a sink has no inputs defined, kill the process

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

    with st.spinner("Simulating E2E DAG Network..."):
        env.run(until=sim_time)
        
    df_q = pd.DataFrame(queue_data)

    # --- DYNAMIC PARALLEL GRAPHVIZ GENERATOR ---
    def generate_dag_vsm():
        dot = graphviz.Digraph(node_attr={'shape': 'box', 'style': 'filled', 'color': '#E1E4E8', 'fontname': 'Helvetica'})
        dot.attr(rankdir='LR', splines='ortho')
        
        # Draw Output Buffers (Yellow Cylinders) & Sinks/Sources
        for _, row in edited_nodes.iterrows():
            nid = row["Node_ID"]
            ntype = row["Type"]
            final_wip = df_q[f"{nid}_WIP"].iloc[-1]
            
            if ntype == "Source":
                dot.node(f'{nid}_Src', f'{nid}\n(Source)\nμ={row["Mean_Mins"]}m', color='#cce5ff', shape='folder')
                dot.node(f'{nid}_Buf', f'{nid} WIP\nQty: {final_wip}', color='#fff3cd', shape='cylinder')
                dot.edge(f'{nid}_Src', f'{nid}_Buf')
            elif ntype == "Sink":
                dot.node(nid, f'{nid}\n(Sink)\nTotal Finished: {final_wip}', color='#d4edda', shape='folder')
            else:
                dot.node(f'{nid}_Buf', f'{nid} Output WIP\nQty: {final_wip}', color='#fff3cd', shape='cylinder')
                
        # Draw Parallel Machines
        for _, row in edited_nodes.iterrows():
            if row["Type"] == "Machine":
                nid = row["Node_ID"]
                qty = int(row["Machines_Qty"])
                
                with dot.subgraph(name=f'cluster_{nid}') as s:
                    s.attr(style='invis') # Hide the cluster bounding box
                    for i in range(qty):
                        m_id = f'{nid}_M{i}'
                        s.node(m_id, f'{nid} {i+1}\nμ={row["Mean_Mins"]}m', color='#e2e3e5')
                        s.edge(m_id, f'{nid}_Buf') # Machine output flows to its own buffer

        # Draw Routing Edges (BOM logic)
        for _, edge in edited_edges.iterrows():
            frm = edge["From_Node"]
            to = edge["To_Node"]
            req = edge["Qty_Required"]
            
            to_type = edited_nodes[edited_nodes["Node_ID"] == to]["Type"].values[0]
            
            # Route: Upstream Buffer -> Merge Point -> Downstream Parallel Machines
            if to_type == "Machine":
                qty = int(edited_nodes[edited_nodes["Node_ID"] == to]["Machines_Qty"].values[0])
                merge_id = f'merge_{frm}_{to}'
                dot.node(merge_id, '', shape='point', width='0')
                dot.edge(f'{frm}_Buf', merge_id, label=f' Pull: {req}', fontcolor='#0056b3', color='#0056b3')
                
                for i in range(qty):
                    dot.edge(merge_id, f'{to}_M{i}', color='#0056b3')
            
            elif to_type == "Sink":
                dot.edge(f'{frm}_Buf', to, label=f' Pull: {req}', fontcolor='#2ca02c', color='#2ca02c')
                
        return dot

    # --- DASHBOARDS ---
    t1, t2 = st.tabs(["🗺️ E2E Network DAG", "📈 Work-In-Progress Physics"])
    
    with t1:
        st.subheader("Dynamic Bill of Materials & Parallel Flow")
        st.markdown("Notice how the upstream WIP buffer splits perfectly into the parallel machine capacity, processes, and deposits into the next WIP buffer.")
        st.graphviz_chart(generate_dag_vsm(), use_container_width=True)
        
    with t2:
        st.subheader("Network Bottleneck Migration")
        df_melt = df_q.melt(id_vars="Time (Mins)", var_name="Buffer", value_name="Inventory Level")
        df_melt_wip = df_melt[~df_melt["Buffer"].str.contains("Finished_Goods")]
        
        fig = px.line(df_melt_wip, x="Time (Mins)", y="Inventory Level", color="Buffer")
        st.plotly_chart(fig, use_container_width=True)
        
        st.subheader("Cumulative Yield")
        sink_nodes = edited_nodes[edited_nodes["Type"] == "Sink"]["Node_ID"].tolist()
        if sink_nodes:
            fg_col = f"{sink_nodes[0]}_WIP"
            fig_fg = px.area(df_q, x="Time (Mins)", y=fg_col, color_discrete_sequence=['#2ca02c'])
            st.plotly_chart(fig_fg, use_container_width=True)

else:
    st.info("👈 Add/Edit your Nodes and Edges, then hit Run.")
