CREATE PARTITION FUNCTION pf_fechas(datetime)
AS RANGE right
FOR VALUES('01/01/2000','01/01/2010', '01/01/2015')

CREATE PARTITION SCHEME ps_ventas
AS PARTITION pf_fechas
TO ('FG05', 'FG02','FG04', 'FG03')

CREATE TABLE pedidosTest(
id int identity,
fechapedido datetime,
monto decimal(9,2),
igv decimal(9,2),
subtotal decimal(9,2)
)
ON ps_ventas(fechapedido)

insert into pedidosTest values ('01/01/1999', 1000, 180, 1180)
select * from pedidosTest
GO 

--particion merge
alter partition function pf_fechas()
merge range('01/01/2000')
GO

--particion split
alter partition scheme ps_ventas
next used FG02

alter partition function pf_fechas()
split range('01/01/2018')
GO

declare @NombreTabla sysname = 'pedidosTest'

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



------------------------------------------------------------------------------------------------------------------------

sp_help pedidosTest

CREATE PARTITION FUNCTION pf_particion01(int)
AS RANGE right
FOR VALUES(1,100,1000,10000)

drop partition scheme ep_esquema01

CREATE PARTITION SCHEME ep_esquema01
AS PARTITION pf_particion01
TO ('FG05', 'FG02','FG04', 'FG03', 'FG01')

CREATE TABLE VentasParticiones(
id int NOT NULL PRIMARY KEY,
fecha datetime NOT NULL,
cliente_id int NOT NULL,
total decimal(6,2) NOT NULL,
subtotal decimal(6,2) NOT NULL,
igv decimal(6,2) NOT NULL
)
ON ep_esquema01(id)

insert into ventasparticiones values(10, '20000120', 1, 118, 100,18)
insert into ventasparticiones values(50, '20000120', 1, 118, 100,18)
insert into ventasparticiones values(100, '20000120', 1, 118, 100,18)
insert into ventasparticiones values(150, '20000120', 1, 118, 100,18)
insert into ventasparticiones values(1000, '20000120', 1, 118, 100,18)
insert into ventasparticiones values(1500, '20000120', 1, 118, 100,18)
insert into ventasparticiones values(10000, '20000120', 1, 118, 100,18)
insert into ventasparticiones values(10001, '20000120', 1, 118, 100,18)
insert into ventasparticiones values(0, '20000120', 1, 118, 100,18)
GO

----------------------------------------------------------------------------------------------------------------------------------------

CREATE PARTITION FUNCTION pf_vtas(int)
AS RANGE right
FOR VALUES(1,100)
GO

drop partition scheme ep_vtas
GO

CREATE PARTITION SCHEME ep_vtas
AS PARTITION pf_vtas
TO ('FG05', 'FG02','FG04')
GO


CREATE NONCLUSTERED INDEX IX_TABLE1_partitioncol
  ON ventas (id)
  WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF,
         ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)
  ON ep_vtas(id)
GO

---------------------------------------------------------------------------------------------------------------------------------------