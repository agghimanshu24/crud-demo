import streamlit as st
from databricks import sql
import pandas as pd


# ---------------- DB CONNECTION ---------------- #

@st.cache_resource
def get_connection():
    return sql.connect(
        server_hostname="dbc-a49f6e9d-ba1d.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/96c844134aa6f9ca",
        access_token="dapi8a2353b4ad6f949ef4acea61f2f58877"
    )


try:
    conn = get_connection()
except Exception as e:
    st.error("Failed to connect to Unity Catalog")
    st.exception(e)
    st.stop()


# Change this to your real table
TABLE_NAME = "main.streamlit_app.users"



# ---------------- HELPER FUNCTIONS ---------------- #

def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        try:
            return cur.fetchall()
        except:
            return None


# ---------------- STREAMLIT APP ---------------- #

def main():

    st.title("CRUD Operations With Unity Catalog")

    menu = st.sidebar.selectbox(
        "Select Operation",
        ("Create", "Read", "Update", "Delete")
    )


    # -------- CREATE -------- #
    if menu == "Create":

        st.subheader("Create Record")

        name = st.text_input("Name")
        email = st.text_input("Email")

        if st.button("Create"):

            query = f"""
            INSERT INTO {TABLE_NAME}
            VALUES (
                (SELECT COALESCE(MAX(id),0)+1 FROM {TABLE_NAME}),
                '{name}',
                '{email}'
            )
            """

            run_query(query)

            st.success("Record Created Successfully")


    # -------- READ -------- #
    elif menu == "Read":

        st.subheader("Read Records")

        query = f"SELECT * FROM {TABLE_NAME}"

        data = run_query(query)

        if data:
            df = pd.DataFrame(data, columns=["id", "name", "email"])
            st.dataframe(df)


    # -------- UPDATE -------- #
    elif menu == "Update":

        st.subheader("Update Record")

        id_val = st.number_input("ID", min_value=1)
        name = st.text_input("New Name")
        email = st.text_input("New Email")

        if st.button("Update"):

            query = f"""
            UPDATE {TABLE_NAME}
            SET name='{name}', email='{email}'
            WHERE id={id_val}
            """

            run_query(query)

            st.success("Record Updated")


    # -------- DELETE -------- #
    elif menu == "Delete":

        st.subheader("Delete Record")

        id_val = st.number_input("ID", min_value=1)

        if st.button("Delete"):

            query = f"DELETE FROM {TABLE_NAME} WHERE id={id_val}"

            run_query(query)

            st.success("Record Deleted")


# ---------------- MAIN ---------------- #

st.set_page_config(page_title="CRUD App", layout="wide")

st.title("CRUD Operations With Unity Catalog")

menu = st.sidebar.selectbox(
    "Select Operation",
    ("Create", "Read", "Update", "Delete")
)
