

def connect_to_database():
    jdbc_url = "jdbc:postgresql://localhost:5432/Taxi_2024_warhouse"
    db_properties = {
        "user": "postgres",
        "password": 123,
        "driver": "org.postgresql.Driver"
    }
    return jdbc_url,db_properties

