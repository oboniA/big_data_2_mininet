import mysql.connector
from mysql.connector import Error
from prettytable import PrettyTable


def create_connection(host_name, database_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            database=database_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection


def create_table(results, cursor):
    """will return the executed queries in tabular form"""
    table = PrettyTable()
    table.field_names = [column[0] for column in cursor.description]  # fethces column names from column description

    for row in results:
        table.add_row(row)  # Adds each row of the query results to the table

    return table

def execute_query(connection, query, params=None):
    cursor = connection.cursor()
    try:
        cursor.execute(query, params)
        results = cursor.fetchall()
        table = create_table(results, cursor)  # Create the table
        print(table)
        print("Query successful")
        cursor.close()
    except Error as err:
        print(f"Error: '{err}'")
        

def query_1(connection, sub_type):
    """Exports all data about users in the HD subscriptions."""

    query = f"""
            SELECT *
            FROM Users U
            WHERE Subscription_type = %s;
    """
    execute_query(connection, query, (sub_type,))


def query_2(connection):
    """Exports all data about actors and their associated movies."""

    query = f"""
            SELECT 
                A.Actor_id AS ActorID,
                A.Name AS Name,
                A.City AS City,
                A.DOB AS DateOfBirth,
                M.Movie_id AS MovieID,
                M.Title AS Title,
                M.Category AS Genre,
                M.ReleaseDate AS ReleaseDate,
                MA.Role AS Role
            FROM Actors A
            JOIN MovieActors MA ON A.Actor_id = MA.Actor_id
            JOIN Movies M ON MA.Movie_id = M.Movie_id;
    """
    execute_query(connection, query)


def query_3(connection, city):
    """Exports all data to group actors from a specific city, showing also the average age (per city)."""
    
    query = f"""
            SELECT
                City,
                COUNT(Actor_id) AS NumberOfActors,
                ROUND(AVG(YEAR(CURRENT_DATE) - YEAR(DOB)),2) AS AverageAge
            FROM Actors
            WHERE City = %s;
    """
    execute_query(connection, query, (city,))


def query_4(connection, user):
    """Exports all data to show the favourite comedy movies for a specific user."""
    
    query = f"""
            SELECT
                U.username AS Username,
                M.Movie_id AS MovieID,
                M.Title AS Title,
                M.Category AS Genre, 
                M.ReleaseDate AS ReleaseDate
            FROM Users U
            JOIN FavoriteMovies FM ON U.User_id = FM.User_id
            JOIN Movies M ON FM.Movie_id = M.Movie_id
            WHERE U.Username = %s AND M.Category = 'Comedy';
    """
    execute_query(connection, query, (user,))


def query_5(connection):
    """Exports  all data to count how many subscriptions are in the database per country."""
    
    query = f"""
            SELECT
                U.Country ,
                COUNT(S.sub_id) AS SubscriptionCount
            FROM Subscriptions S
            JOIN Users U ON S.sub_id = U.sub_id
            GROUP BY U.Country;
    """
    execute_query(connection, query)


def main():
    host = "34.171.50.39" # host IP address for pwd-mininet-sql from GCP
    database = "mininet_db" # database name
    user = "root" # default user 
    password = "123456789" # user password
    connection = create_connection(host, database, user, password) # establishes connection

    if connection:
        try:
            while True:
                print("\nSelect a query to run:")
                print("1. Export all data about users in the HD or UHD subscriptions.")
                print("2. Export all data about actors and their associated movies.")
                print("3. Export all data to group actors from a specific city, showing also the average age (per city).")
                print("4. Export all data to show the favourite comedy movies for a specific user.")
                print("5. Export all data to count how many subscriptions are in the database per country.")
                print("e. Exit")
                choice = input("Enter your choice: ") # user input to choose query (1-5)

                if choice == '1':
                    sub_type = input("Enter the subscription (HD/UHD) of your choice: ")
                    query_1(connection, sub_type)
                elif choice == '2':
                    query_2(connection)
                elif choice == '3':
                    city = input("Enter the specific city of your choice: ")
                    query_3(connection, city)
                elif choice == '4':
                    user = input("Enter the specific user of your choice: ")
                    query_4(connection, user)
                elif choice == '5':
                    query_5(connection)
                elif choice == 'e':
                    break
                else: 
                    print("Invalid choice!")

        except Error as e:
            print("Connection Not Available.")


if __name__ == "__main__":
    main()




