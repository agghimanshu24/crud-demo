import streamlit as st
from databricks import sql
import pandas as pd

@st.cache_resource
def get_connection():
    return sql.connect(
        server_hostname="dbc-a49f6e9d-ba1d.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/96c844134aa6f9ca",
        access_token="dapi8a2353b4ad6f949ef4acea61f2f58877"
    )

def run_query(query, params=None):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(query, params or ())
        try:
            return cur.fetchall()
        except:
            return None

TABLE_NAME = "main.streamlit_app.users"

def main():
    st.set_page_config(page_title="CRUD App", layout="wide")
    st.title("CRUD Operations With Unity Catalog")

    menu = st.sidebar.selectbox("Select Operation", ("Create", "Read", "Update", "Delete"))

    if menu == "Create":
        st.subheader("Create Record")
        name = st.text_input("Name")
        email = st.text_input("Email")
        if st.button("Create"):
            # First get the next ID
            id_query = f"SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM {TABLE_NAME}"
            result = run_query(id_query)
            next_id = result[0][0] if result else 1

            # Then insert using parameters
            insert_query = f"INSERT INTO {TABLE_NAME} (id, name, email) VALUES (?, ?, ?)"
            run_query(insert_query, (next_id, name, email))
            st.success("Record Created Successfully")

    elif menu == "Read":
        st.subheader("Read Records")
        query = f"SELECT * FROM {TABLE_NAME}"
        data = run_query(query)
        if data:
            df = pd.DataFrame(data, columns=["id", "name", "email"])
            st.dataframe(df)

    elif menu == "Update":
        st.subheader("Update Record")
        id_val = st.number_input("ID", min_value=1)
        name = st.text_input("New Name")
        email = st.text_input("New Email")
        if st.button("Update"):
            query = f"UPDATE {TABLE_NAME} SET name=?, email=? WHERE id=?"
            run_query(query, (name, email, id_val))
            st.success("Record Updated")

    elif menu == "Delete":
        st.subheader("Delete Record")
        id_val = st.number_input("ID", min_value=1)
        if st.button("Delete"):
            query = f"DELETE FROM {TABLE_NAME} WHERE id=?"
            run_query(query, (id_val,))
            st.success("Record Deleted")

if __name__ == "__main__":
    main()
