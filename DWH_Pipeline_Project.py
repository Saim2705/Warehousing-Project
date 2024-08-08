from dateutil import parser
import mysql.connector
import pandas as pd
import csv
import matplotlib.pyplot as plt

#Database connection parameters
db_config = {
    'host': 'localhost',
    'database': 'rdbms',
    'user': 'root',  # change it with your username
    'password': ''  # change it with your password
}
db_config2 = {
    'host': 'localhost',
    'database': 'star_schema',
    'user': 'root',  # change it with your username
    'password': ''  # change it with your password
}

#data insertion into RDBMS database

def data_insert_movie(cursor1, connection1):

    df = pd.read_csv('Movie.csv')

    for index, row in df.iterrows():
        query = "INSERT INTO movie (Movie_ID, Movie_Title, Released_Year, Runtime, Genre_Codes, Producer_ID, Distributor_ID, Gross) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        values = (row['Movie_ID'], row['Movie_Title'], row['Released_Year'], row['Runtime'], row['Genre_Codes'], row['Producer_ID'],
                  row['Distributor_ID'], row['Gross'])
        cursor1.execute(query, values)
        connection1.commit()
    print("Movie data inserted in RDBMS")

def data_insert_company(cursor1, connection1):
    df = pd.read_csv('Company.csv')

    for index, row in df.iterrows():
        query = "INSERT INTO company (Company_ID, Company_Name, Country) VALUES (%s, %s, %s)"
        values = (row['Company_ID'], row['Company_Name'], row['Country'])
        cursor1.execute(query, values)
        connection1.commit()

    print("Company data inserted into RDBMS")

def data_insert_employee(cursor1, connection1):
    df = pd.read_csv('Employee.csv')

    for index, row in df.iterrows():
        query = "INSERT INTO employee (Employee_ID, Employee_Name) VALUES (%s, %s)"
        # Pass the values to be inserted in the query
        values = (row['Employee_ID'], row['Employee_Name'] )
        cursor1.execute(query, values)
        connection1.commit()
    print("Employee data inserted into RDBMS")

def data_insert_rating(cursor1, connection1):
    df = pd.read_csv('Rating.csv')

    for index, row in df.iterrows():
        query = "INSERT INTO rating (Rating_ID, Movie_ID, Rotten_Tomatoes, IMDB, Metacritic) VALUES (%s, %s, %s, %s, %s)"
        # Pass the values to be inserted in the query
        values = (row['Movie_ID'], row['Movie_ID'], row['Rotten_Tomatoes'], row['IMDB'], row['Metacritic'])
        cursor1.execute(query, values)
        connection1.commit()

    print("Rating data inserted in RDBMS")

def data_insert_show(cursor1, connection1):
    df = pd.read_csv('Show.csv')

    for index, row in df.iterrows():
        query = "INSERT INTO shows (Show_ID, Movie_ID, Screen_ID, Employee_OnDuty, Date, Time) VALUES (%s, %s, %s, %s, %s, %s)"
        values = (row['Show_ID'], row['Movie_ID'], row['Screen_ID'], row['Employee_OnDuty'], parser.parse(row['Date']), row['Time'])
        cursor1.execute(query, values)
        connection1.commit()

    print("Show data inserted in RDBMS")

def data_insert_show_stats(cursor1, connection1):
    df = pd.read_csv('Show_Stats.csv')

    for index, row in df.iterrows():
        query = "INSERT INTO show_stats (Show_ID, Attendance, Age_Demographic, Gender_Demographic) VALUES (%s, %s, %s, %s)"
        values = (row['Show_ID'], row['Attendance'], row['Age_Demographic'],  row['Gender_Demographic'])
        cursor1.execute(query, values)
        connection1.commit()

    print("Show_Stats data inserted in RDBMS")


def data_insert_genre(cursor1, connection1):

    df = pd.read_csv('Genre.csv')

    for index, row in df.iterrows():
        query = "INSERT INTO genre (Genre_ID, Genre_Name) VALUES (%s, %s)"
        values = (row['Genre_ID'], row['Genre_Name'].strip(), )
        cursor1.execute(query, values)
        connection1.commit()
    print("Genre data inserted into RDBMS")

def data_insert_movie_to_actor(cursor1, connection1):

    df = pd.read_csv('Movie_to_Actor.csv')

    for index, row in df.iterrows():
        query = ("INSERT INTO movie_to_actor (Movie_ID, Actor_1_ID, Actor_2_ID, Actor_3_ID, Actor_4_ID) VALUES (%s, %s, %s, %s, %s)")
        values = ((int(row['Movie_ID'])), (int(row['Actor_1_ID'])),  (int(row['Actor_2_ID'])),  (int(row['Actor_3_ID'])),  (int(row['Actor_4_ID'])))
        cursor1.execute(query, values)
        connection1.commit()
    print("Movie_to_Actor data inserted into RDBMS")

def data_insert_movie_to_certificate(cursor1, connection1):

    df = pd.read_csv('Movie_to_Certificate.csv')


    for index, row in df.iterrows():
        query = ("INSERT INTO movie_to_certificate (Movie_ID, Certificate_ID) VALUES (%s, %s)")
        values = (int(row['Movie_ID']),  (int(row['Certificate_ID'])))
        cursor1.execute(query, values)
        connection1.commit()
    print("Movie_to_Certificate data inserted into RDBMS")

def data_insert_screen(cursor1, connection1):
    df = pd.read_csv('Screen.csv')

    for index, row in df.iterrows():
        query = "INSERT INTO screen (Screen_ID, Capacity, Video_System, Sound_System) VALUES (%s, %s, %s, %s)"
        values = (row['Screen_ID'], row['Capacity'], row['Video_System'], row['Sound_System'])
        cursor1.execute(query, values)
        connection1.commit()

    print("Screen data inserted into RDBMS")

def data_insert_actor(cursor1, connection1):
    df = pd.read_csv('Actor.csv')

    for index, row in df.iterrows():
        query = "INSERT INTO actor (Actor_ID, Actor_Name, Gender, Age) VALUES (%s, %s, %s, %s)"
        values = (row['Actor_ID'], row['Actor_Name'], row['Gender'], row['Age'])
        cursor1.execute(query, values)
        connection1.commit()

    print("Actor data inserted into RDBMS")

def data_insert_certificate(cursor1, connection1):
    df = pd.read_csv('Certificate.csv')

    for index, row in df.iterrows():
        query = "INSERT INTO certificate (Certificate_ID, Certificate_Rating, Certification_Body) VALUES (%s, %s, %s)"
        values = (row['Certificate_ID'], row['Certificate_Rating'], row['Certification_Body'])
        cursor1.execute(query, values)
        connection1.commit()

    print("Certificate data inserted into RDBMS")


#Data extraction from RDBMS database and insertion into star-schema

def data_extract_insert_movie_dim(cursor1, cursor2, connection2):
    query = (
        "SELECT movie.Movie_ID AS Movie_ID, movie.Movie_Title AS Movie_Title, g.Genre_Name AS Genre, "
        "c1.Company_Name AS Producer, c2.Company_Name AS Distributor "
        "FROM movie "
        "LEFT JOIN genre AS g ON movie.Genre_Codes = g.Genre_ID "
        "LEFT JOIN company AS c1 ON c1.Company_ID = movie.Producer_ID "
        "LEFT JOIN company AS c2 ON c2.Company_ID = movie.Distributor_ID"
    )
    cursor1.execute(query)
    data = cursor1.fetchall()

    insert_query = (
        "INSERT INTO movie_dim (Movie_ID, Movie_Title, Genre, Producer, Distributor) "
        "VALUES (%s, %s, %s, %s, %s)"
    )
    for row in data:
        cursor2.execute(insert_query, (row[0], row[1], row[2], row[3], row[4]))

    connection2.commit()
    print("Data ectracted from RDBMS and inserted into Star_Schema movie_dim table")

def data_extract_insert_screen_dim(cursor1, cursor2, connection2):
    query = (
        "SELECT Screen_ID, Capacity, Video_System, "
        "Sound_System "
        "FROM screen "
    )
    cursor1.execute(query)
    data = cursor1.fetchall()

    insert_query = (
        "INSERT INTO screen_dim (Screen_ID, Capacity, Video_System, Sound_System) "
        "VALUES (%s, %s, %s, %s)"
    )
    for row in data:
        cursor2.execute(insert_query, (row[0], row[1], row[2], row[3]))

    connection2.commit()
    print("Data extracted from RDBMS and inserted into Star_Schema Screen_dim table")

def data_extract_insert_rating_certification_dim(cursor1, cursor2, connection2):
    query = (
        "SELECT movie.Movie_ID, r.Rotten_Tomatoes, r.IMDB, "
        "r.Metacritic, c.Certificate_Rating, c.Certification_Body "
        "FROM movie "
        "LEFT JOIN rating AS r ON movie.Movie_ID = r.Movie_ID "
        "LEFT JOIN movie_to_certificate AS mc ON movie.Movie_ID = mc.Movie_ID "
        "LEFT JOIN certificate AS c ON mc.Certificate_ID = c.Certificate_ID"
    )
    cursor1.execute(query)
    data = cursor1.fetchall()

    insert_query = (
        "INSERT INTO rating_certification_dim (R_C_ID, Rotten_Tomatoes, IMDB, Metacritic, Certificate_Rating, Certificate_Body) "
        "VALUES (%s, %s, %s, %s, %s, %s)"
    )
    for row in data:
        cursor2.execute(insert_query, (row[0], row[1], row[2], row[3], row[4], row[5]))

    connection2.commit()
    print("Data extracted form RDBMS and inserted into Star_Schema Rating_Certification_dim table")

def data_extract_insert_show_fact_table(cursor1, cursor2, connection2):
        query = (
            "SELECT s.Show_ID, m.Movie_ID, sc.Screen_ID, "
            "s.Date, s.Time, st.Attendance, st.Gender_Demographic, st.Age_Demographic "
            "FROM shows as s "
            "LEFT JOIN movie as m ON s.Movie_ID = m.Movie_ID "
            "LEFT JOIN screen as sc ON s.Screen_ID = sc.Screen_ID "
            "LEFT JOIN show_stats as st ON s.Show_ID = st.Show_ID"
        )
        cursor1.execute(query)
        data = cursor1.fetchall()

        insert_query = (
            "INSERT INTO show_fact_table (Show_ID, Movie_ID, Screen_ID, Date, R_C_ID, Time, Attendance, Gender_Demographic, Age_Demographic) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )

        for row in data:
            total_seconds = row[4].total_seconds()
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            time = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

            cursor2.execute(insert_query, (row[0], row[1], row[2], row[3], row[1], time, row[5], row[6], row[7]))

        connection2.commit()
        print("Data extracted from dimension tables of Star_Schema and inserted into show_fact_table of Star_Schema")

def data_extract_CSV_From_Star_Schema(cursor2):
    query = (
        "SELECT  "
        "sft.Date, sft.Time, sft.Attendance, sft.Gender_Demographic, sft.Age_Demographic, "
        "md.Movie_Title, md.Genre, md.Producer, md.Distributor, "
        "rcd.Rotten_Tomatoes, rcd.IMDB, rcd.Metacritic, rcd.Certificate_Rating, rcd.Certificate_Body, "
        "sd.Capacity, sd.Video_System, sd.Sound_System "
        "FROM show_fact_table as sft "
        "LEFT JOIN movie_dim as md ON sft.Movie_ID = md.Movie_ID "
        "LEFT JOIN screen_dim as sd ON sft.Screen_ID = sd.Screen_ID "
        "LEFT JOIN rating_certification_dim as rcd ON sft.R_C_ID = rcd.R_C_ID"
    )
    cursor2.execute(query)
    data = cursor2.fetchall()

    with open('Final.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        # Write the header row
        writer.writerow([
            'Date', 'Time', 'Attendance', 'Gender_Demographic', 'Age_Demographic',
            'Movie_Title', 'Genre', 'Producer', 'Distributor',
            'Rotten_Tomatoes', 'IMDB', 'Metacritic', 'Certificate_Rating', 'Certificate_Body',
            'Capacity', 'Video_System', 'Sound_System'
        ])

        for row in data:
            total_seconds = row[1].total_seconds()
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            time = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"
            writer.writerow([
                row[0], time, row[2], row[3], row[4],
                row[5], row[6], row[7], row[8],
                row[9], row[10], row[11], row[12], row[13],
                row[14], row[15], row[16]
            ])
    print("Final CSV file is ready for plotting as Final.csv")

def business_query_execute_Star_Schema(cursor2):
    query = "select count(Show_ID) from show_fact_table where Movie_ID = 100"
    query2 = "SELECT * from show_fact_table where show_fact_table.Gender_Demographic < 0.5"
    query3 = "SELECT sum(Attendance) from show_fact_table where show_fact_table.Movie_ID = 12"
    query4 = "SELECT Distributor, COUNT(Movie_ID) as Movie_Count FROM movie_dim GROUP BY Distributor ORDER BY Movie_Count desc LIMIT 1"
    cursor2.execute(query)
    print(cursor2.fetchall())

def plot_graphs():
    try:
        # Load the dataset
        df = pd.read_csv('Final_1.csv', encoding='latin1')

        # Check for empty dataframe
        if df.empty:
            print("The DataFrame is empty. Please check the CSV file.")
            return

        # Bar Chart: Producer and IMDB
        if 'Producer' in df.columns and 'IMDB' in df.columns:
            plt.figure(figsize=(12, 8))
            producer_imdb = df.groupby('Producer')['IMDB'].mean()
            if not producer_imdb.empty:
                producer_imdb.plot(kind='bar', color='skyblue')
                plt.title('Average IMDB Rating by Producer')
                plt.xlabel('Producer')
                plt.ylabel('Average IMDB Rating')
                plt.xticks(rotation=90)
                plt.grid(True)
                plt.tight_layout()
                plt.show()
            else:
                print("No data available for 'Producer' and 'IMDB'.")
        else:
            print("Columns 'Producer' or 'IMDB' not found in the DataFrame.")


        # Pie Chart: Genre
        if 'Genre' in df.columns:
            plt.figure(figsize=(12, 8))
            genre_counts = df['Genre'].value_counts()
            if not genre_counts.empty:
                genre_counts.plot(kind='pie', autopct='%1.1f%%', startangle=140)
                plt.title('Genre Distribution')
                plt.ylabel('')
                plt.tight_layout()
                plt.savefig("Pie_Chart_Half_Dataset")
                plt.show()
            else:
                print("No data available for 'Genre'.")
        else:
            print("Column 'Genre' not found in the DataFrame.")

        print("Graphs plotted successfully.")

    except Exception as e:
        print(f"Error while plotting graphs: {e}")
# Main function
def main():

    # Connecting to both the databases
    try:
        with mysql.connector.connect(**db_config) as connection1:
            print('Connected to MySQL database RDBMS')
            with connection1.cursor() as cursor1:
                with mysql.connector.connect(**db_config2) as connection2:
                    print('Connected to MySQL database Star_Schema')
                    with connection2.cursor() as cursor2:
                    #      data_insert_company(cursor1, connection1)
                    #     data_insert_genre(cursor1, connection1)
                    #     data_insert_actor(cursor1, connection1)
                    #     data_insert_certificate(cursor1, connection1)
                    #     data_insert_screen(cursor1, connection1)
                    #     data_insert_employee(cursor1, connection1)
                    #     data_insert_movie(cursor1, connection1)
                    #     data_insert_rating(cursor1, connection1)
                    #     data_insert_show(cursor1, connection1)
                    #     data_insert_show_stats(cursor1, connection1)
                    #     data_insert_movie_to_actor(cursor1, connection1)
                    #     data_insert_movie_to_certificate(cursor1, connection1)
                    #     data_extract_insert_movie_dim(cursor1, cursor2, connection2)
                    #     data_extract_insert_screen_dim(cursor1, cursor2, connection2)
                    #     data_extract_insert_rating_certification_dim(cursor1, cursor2, connection2)
                    #     data_extract_insert_show_fact_table(cursor1, cursor2, connection2)
                    #      data_extract_CSV_From_Star_Schema(cursor2)
                            business_query_execute_Star_Schema(cursor2)
    except mysql.connector.Error as e:
         print(f"Error connecting to MySQL database 2: {e}")

    plot_graphs()

# Run the main function
if __name__ == "__main__":
    main()
