
-- ============================================

-- VARIABLE BIND: AÑO A PROCESAR

VARIABLE b_anno NUMBER;
EXEC :b_anno := EXTRACT(YEAR FROM SYSDATE);

DECLARE

    -- VARRAY: TIPOS DE TRANSACCIÓN
    TYPE varray_tipos IS VARRAY(2) OF VARCHAR2(40);
    v_tipos varray_tipos := varray_tipos(
        'Avance en Efectivo',
        'Súper Avance en Efectivo'
    );

    -- REGISTRO PARA DETALLE
    TYPE r_transaccion IS RECORD (
        numrun            cliente.numrun%TYPE,
        dvrun             cliente.dvrun%TYPE,
        nro_tarjeta       tarjeta_cliente.nro_tarjeta%TYPE,
        nro_transaccion   transaccion_tarjeta_cliente.nro_transaccion%TYPE,
        fecha_transaccion transaccion_tarjeta_cliente.fecha_transaccion%TYPE,
        tipo_transaccion  tipo_transaccion_tarjeta.nombre_tptran_tarjeta%TYPE,
        monto_total       transaccion_tarjeta_cliente.monto_total_transaccion%TYPE
    );

    v_reg r_transaccion;

    -- VARIABLES DE CONTROL
    v_porcentaje NUMBER;
    v_aporte     NUMBER;
    v_total      NUMBER := 0;
    v_procesados NUMBER := 0;

    -- EXCEPCIÓN DEFINIDA POR EL USUARIO
    e_monto_fuera_tramo EXCEPTION;

    -- CURSOR DETALLE
    CURSOR c_detalle IS
        SELECT c.numrun, c.dvrun, tc.nro_tarjeta,
               ttc.nro_transaccion, ttc.fecha_transaccion,
               tpt.nombre_tptran_tarjeta,
               ttc.monto_total_transaccion
        FROM cliente c
        JOIN tarjeta_cliente tc
          ON c.numrun = tc.numrun
        JOIN transaccion_tarjeta_cliente ttc
          ON tc.nro_tarjeta = ttc.nro_tarjeta
        JOIN tipo_transaccion_tarjeta tpt
          ON ttc.cod_tptran_tarjeta = tpt.cod_tptran_tarjeta
        WHERE EXTRACT(YEAR FROM ttc.fecha_transaccion) = :b_anno
          AND tpt.nombre_tptran_tarjeta IN (
              'Avance en Efectivo',
              'Súper Avance en Efectivo'
          )
        ORDER BY ttc.fecha_transaccion, c.numrun;

    -- CURSOR RESUMEN
    CURSOR c_resumen (p_mes_anno VARCHAR2, p_tipo VARCHAR2) IS
        SELECT SUM(monto_transaccion),
               SUM(aporte_sbif)
        FROM detalle_aporte_sbif
        WHERE TO_CHAR(fecha_transaccion,'MMYYYY') = p_mes_anno
          AND tipo_transaccion = p_tipo;

    v_sum_monto  NUMBER;
    v_sum_aporte NUMBER;

BEGIN
    -- LIMPIEZA DE TABLAS DESTINO
    EXECUTE IMMEDIATE 'TRUNCATE TABLE detalle_aporte_sbif';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE resumen_aporte_sbif';

    -- TOTAL DE REGISTROS ESPERADOS
    SELECT COUNT(*)
    INTO v_total
    FROM transaccion_tarjeta_cliente ttc
    JOIN tipo_transaccion_tarjeta tpt
      ON ttc.cod_tptran_tarjeta = tpt.cod_tptran_tarjeta
    WHERE EXTRACT(YEAR FROM ttc.fecha_transaccion) = :b_anno
      AND tpt.nombre_tptran_tarjeta IN (
          'Avance en Efectivo',
          'Súper Avance en Efectivo'
      );

    -- PROCESO DETALLE
    OPEN c_detalle;
    LOOP
        FETCH c_detalle INTO v_reg;
        EXIT WHEN c_detalle%NOTFOUND;

        BEGIN
            SELECT porc_aporte_sbif
            INTO v_porcentaje
            FROM tramo_aporte_sbif
            WHERE v_reg.monto_total
                  BETWEEN tramo_inf_av_sav AND tramo_sup_av_sav;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE e_monto_fuera_tramo;
        END;

        v_aporte := ROUND(v_reg.monto_total * v_porcentaje / 100);

        INSERT INTO detalle_aporte_sbif
        VALUES (
            v_reg.numrun,
            v_reg.dvrun,
            v_reg.nro_tarjeta,
            v_reg.nro_transaccion,
            v_reg.fecha_transaccion,
            v_reg.tipo_transaccion,
            v_reg.monto_total,
            v_aporte
        );

        v_procesados := v_procesados + 1;
    END LOOP;
    CLOSE c_detalle;

    -- PROCESO RESUMEN ----       
    FOR m IN 1..12 LOOP
        FOR t IN 1..v_tipos.COUNT LOOP
            DECLARE
                v_periodo VARCHAR2(6) := LPAD(m,2,'0') || :b_anno;
            BEGIN
                OPEN c_resumen(v_periodo, v_tipos(t));
                FETCH c_resumen INTO v_sum_monto, v_sum_aporte;
                CLOSE c_resumen;

                IF v_sum_monto IS NOT NULL THEN
                    INSERT INTO resumen_aporte_sbif
                    VALUES (
                        v_periodo,
                        v_tipos(t),
                        v_sum_monto,
                        v_sum_aporte
                    );
                END IF;
            END;
        END LOOP;
    END LOOP;

    -- CONTROL TRANSACCIONAL ----
    IF v_procesados = v_total AND v_total > 0 THEN
        COMMIT;
    ELSE
        ROLLBACK;
    END IF;

EXCEPTION
    WHEN e_monto_fuera_tramo THEN
        ROLLBACK;
    WHEN OTHERS THEN
        ROLLBACK;
END;
/

-- REVISIÓN TABLA DETALLE_APORTE_SBIF 
SELECT * 
FROM detalle_aporte_sbif 
ORDER BY fecha_transaccion, numrun;

-- REVISIÓN TABLA RESUMEN_APORTE_SBIF
SELECT *
FROM resumen_aporte_sbif
ORDER BY mes_anno, tipo_transaccion;