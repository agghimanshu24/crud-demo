import psycopg2
import streamlit as st

# ---------------- DB CONNECTION ---------------- #

mydb = psycopg2.connect(
    host="localhost",
    user="postgres",        
    password="password",   
    dbname="crud_new1",
    port="5432"
)

mycursor = mydb.cursor()

st.write("Connection Established with PostgreSQL")


# ---------------- STREAMLIT APP ---------------- #

def main():

    st.title("CRUD Operations With PostgreSQL")

    # Sidebar Menu
    option = st.sidebar.selectbox(
        "Select an Operation",
        ("Create", "Read", "Update", "Delete")
    )

    # ---------------- CREATE ---------------- #
    if option == "Create":

        st.subheader("Create a Record")

        name = st.text_input("Enter Name")
        email = st.text_input("Enter Email")

        if st.button("Create"):

            sql = "INSERT INTO users(name, email) VALUES (%s, %s)"
            val = (name, email)

            mycursor.execute(sql, val)
            mydb.commit()

            st.success("Record Created Successfully!!!")


    # ---------------- READ ---------------- #
    elif option == "Read":

        st.subheader("Read Records")

        mycursor.execute("SELECT * FROM users")
        result = mycursor.fetchall()

        for row in result:
            st.write(row)


    # ---------------- UPDATE ---------------- #
    elif option == "Update":

        st.subheader("Update a Record")

        id = st.number_input("Enter ID", min_value=1, step=1)
        name = st.text_input("Enter New Name")
        email = st.text_input("Enter New Email")

        if st.button("Update"):

            sql = """
            UPDATE users
            SET name = %s, email = %s
            WHERE id = %s
            """

            val = (name, email, id)

            mycursor.execute(sql, val)
            mydb.commit()

            st.success("Record Updated Successfully!!!")


    # ---------------- DELETE ---------------- #
    elif option == "Delete":

        st.subheader("Delete a Record")

        id = st.number_input("Enter ID", min_value=1, step=1)

        if st.button("Delete"):

            sql = "DELETE FROM users WHERE id = %s"
            val = (id,)

            mycursor.execute(sql, val)
            mydb.commit()

            st.success("Record Deleted Successfully!!!")


# ---------------- MAIN ---------------- #

if __name__ == "__main__":
    main()
