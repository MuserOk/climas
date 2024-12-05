-- Crear tabla Weather_db si no existe
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'Weather_db')
BEGIN
    CREATE TABLE Weather_db (
        id INT IDENTITY(1,1) PRIMARY KEY,
        ciudad VARCHAR(100) NOT NULL,
        temperatura FLOAT NOT NULL,
        humedad INT NOT NULL,
        presion INT NOT NULL,
        descripcion_clima VARCHAR(255) NOT NULL,
        velocidad_viento FLOAT NOT NULL,
        marca_temporal DATETIME NOT NULL
    );
END
