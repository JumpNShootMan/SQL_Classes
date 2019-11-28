use BaseDatosTienda

--Parte 2
--Pregunta 1
CREATE TABLE alumnos(
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[codigo] [varchar](11) NOT NULL,
	[nombres] [varchar](150) NOT NULL,
	[apellidos] [varchar](150) NOT NULL,
	[sexo] [tinyint] NULL,
	[estado] [bit] NULL
);

DECLARE @i AS bigint = 10000000000;
WHILE @i < 10000050000
BEGIN
SET @i = @i + 1;
INSERT INTO alumnos (codigo, nombres, apellidos, sexo, estado) 
VALUES (CONVERT(varchar(11), @i), 'Nombres ' + CONVERT(varchar(11), @i), 'Apellidos ' + CONVERT(varchar(11), @i), 0, 1);
END;


/*consultas que se ejecutan en la tabla*/
select codigo, apellidos from alumnos where codigo = '12345678901';
select codigo, apellidos from alumnos where id between 1000 and 2000;
select apellidos from alumnos where sexo = 1;
select apellidos from alumnos where estado = 0;

--Para mejorar rendimiento de cada consulta...
create unique nonclustered index nci_cod on alumnos(codigo, apellidos)
create clustered index ci_idapesex on alumnos(id, codigo, apellidos, estado)
create nonclustered index nci_apsex on alumnos(apellidos,sexo)


--Pregunta 2

CREATE PARTITION FUNCTION pf_fechas(date)
AS RANGE right
FOR VALUES('01/01/1985','01/01/1990','01/01/1995')

CREATE PARTITION SCHEME ep_esquema01
AS PARTITION pf_fechas
TO ('GROUP05', 'GROUP01','GROUP03', 'GROUP02')

--drop partition scheme ep_esquema01

CREATE TABLE LecturasDeLuz(
id int,
lecturainicial int,
lecturafinal int,
fechalectura date,
cliente_id bigint
)
ON ep_esquema01(fechalectura)
GO

insert into LecturasDeLuz values(1,2,3,'01/01/1980',4)
insert into LecturasDeLuz values(1,2,3,'01/01/1985',4)
insert into LecturasDeLuz values(1,2,3,'01/01/1990',4)
insert into LecturasDeLuz values(1,2,3,'01/01/1995',4)
insert into LecturasDeLuz values(1,2,3,'01/01/2000',4)

CREATE TABLE LecturasDeLuzHistorico(
id int,
lecturainicial int,
lecturafinal int,
fechalectura date,
cliente_id bigint
) on 'GROUP05'


--particion split
alter partition scheme ep_esquema01
next used GROUP04

alter partition function pf_fechas()
split range('01/01/2010')
GO

--particion merge
alter partition function pf_fechas()
merge range('01/01/1995')

--Insercion de datos de particion 1
INSERT INTO LecturasDeLuzHistorico
SELECT *
FROM LecturasDeLuz
WHERE fechalectura < '1985/01/01';

declare @NombreTabla sysname = 'LecturasDeLuz'
SELECT  pf.name AS pf_name ,
        ps.name AS partition_scheme_name ,
        p.partition_number ,
        ds.name AS partition_filegroup ,
        pf.type_desc AS pf_type_desc ,
        pf.fanout AS pf_fanout ,
        pf.boundary_value_on_right ,
        OBJECT_NAME(si.object_id) AS object_name ,
        rv.value AS range_value ,
        SUM(CASE WHEN si.index_id IN ( 1, 0 ) THEN p.rows
                    ELSE 0
            END) AS num_rows ,
        SUM(dbps.reserved_page_count) * 8 / 1024. AS reserved_mb_all_indexes ,
        SUM(CASE ISNULL(si.index_id, 0)
                WHEN 0 THEN 0
                ELSE 1
            END) AS num_indexes
FROM    sys.destination_data_spaces AS dds
        JOIN sys.data_spaces AS ds ON dds.data_space_id = ds.data_space_id
        JOIN sys.partition_schemes AS ps ON dds.partition_scheme_id = ps.data_space_id
        JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
        LEFT JOIN sys.partition_range_values AS rv ON pf.function_id = rv.function_id
                                                        AND dds.destination_id = CASE pf.boundary_value_on_right
                                                                                    WHEN 0 THEN rv.boundary_id
                                                                                    ELSE rv.boundary_id + 1
                                                                                END
        LEFT JOIN sys.indexes AS si ON dds.partition_scheme_id = si.data_space_id
        LEFT JOIN sys.partitions AS p ON si.object_id = p.object_id
                                            AND si.index_id = p.index_id
                                            AND dds.destination_id = p.partition_number
        LEFT JOIN sys.dm_db_partition_stats AS dbps ON p.object_id = dbps.object_id
                                                        AND p.partition_id = dbps.partition_id

														where OBJECT_NAME(si.object_id) = @NombreTabla
GROUP BY ds.name ,
        p.partition_number ,
        pf.name ,
        pf.type_desc ,
        pf.fanout ,
        pf.boundary_value_on_right ,
        ps.name ,
        si.object_id ,
        rv.value
ORDER BY partition_number
GO

--Pregunta 3

select p.id as "@productoID", p.precio as "@productoPrecio",
p.nombre as "Nombre",
p.stock as "Detalle/Stock",
p.visitas as "Detalle/Visitas",
c.id as "Detalle/Categoria/@id",
c.nombre as "Detalle/Categoria/Nombre",
c.estado as "Detalle/Categoria/Estado",
sum(pc.cantidad) as "Ventas/Cantidad",
sum(pc.precio) as "Ventas/Precio"
from productos p inner join categorias c on c.id = p.categoria_id 
inner join productos_clientes pc on pc.producto_id = p.id
group by p.id, p.precio, p.nombre, p.stock, p.visitas, c.id, c.nombre, c.estado
for xml path('Producto'), root('Ventas'), elements
