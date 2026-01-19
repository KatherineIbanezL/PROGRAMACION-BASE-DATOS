
-- HABILITAR SALIDA DBMS Output

SET SERVEROUTPUT ON;

-- LIMPIEZA DE TABLA
TRUNCATE TABLE usuario_clave;

-- BIND VARIABLE PARA LA FECHA PROCESO
VARIABLE v_fecha_proceso DATE
EXEC :v_fecha_proceso := TO_DATE('01-12-2020','DD-MM-YYYY');

DECLARE
    -- Variable local que referencia la bind variable
    v_fecha_proc DATE := :v_fecha_proceso;

    ------------------------------------------------------------------
    -- Variables con %TYPE
    v_id_emp        empleado.id_emp%TYPE;
    v_run           empleado.numrun_emp%TYPE;
    v_dv            empleado.dvrun_emp%TYPE;
    v_nombre        empleado.pnombre_emp%TYPE;
    v_apellido      empleado.appaterno_emp%TYPE;
    v_estado        estado_civil.nombre_estado_civil%TYPE;
    v_sueldo        empleado.sueldo_base%TYPE;
    v_fecha_ing     empleado.fecha_contrato%TYPE;
    v_fecha_nac     empleado.fecha_nac%TYPE;

    v_usuario       usuario_clave.nombre_usuario%TYPE;
    v_clave         usuario_clave.clave_usuario%TYPE;
    v_nombre_comp   usuario_clave.nombre_empleado%TYPE;

    v_anios_trab    NUMBER;
    v_total_emp     NUMBER;
    v_contador      NUMBER := 0;

    ------------------------------------------------------------------
    -- Cursor ordenado por ID_EMP
    CURSOR c_emp IS
        SELECT e.id_emp,
               e.numrun_emp,
               e.dvrun_emp,
               e.pnombre_emp,
               e.appaterno_emp,
               e.fecha_nac,
               e.fecha_contrato,
               e.sueldo_base,
               ec.nombre_estado_civil
        FROM empleado e
        JOIN estado_civil ec
          ON e.id_estado_civil = ec.id_estado_civil
        WHERE e.id_emp BETWEEN 100 AND 320
        ORDER BY e.id_emp;

BEGIN
    DBMS_OUTPUT.PUT_LINE('--------------------------------------------');
    DBMS_OUTPUT.PUT_LINE('Inicio proceso generación USUARIO_CLAVE');
    DBMS_OUTPUT.PUT_LINE('Fecha de proceso: ' || TO_CHAR(v_fecha_proc,'DD-MM-YYYY'));
    DBMS_OUTPUT.PUT_LINE('--------------------------------------------');

    ------------------------------------------------------------------
    -- Total de empleados a procesar
    SELECT COUNT(*)
    INTO v_total_emp
    FROM empleado
    WHERE id_emp BETWEEN 100 AND 320;

    ------------------------------------------------------------------
    -- Proceso principal
    FOR r IN c_emp LOOP
        v_id_emp    := r.id_emp;
        v_run       := r.numrun_emp;
        v_dv        := r.dvrun_emp;
        v_nombre    := r.pnombre_emp;
        v_apellido  := r.appaterno_emp;
        v_estado    := r.nombre_estado_civil;
        v_sueldo    := r.sueldo_base;
        v_fecha_ing := r.fecha_contrato;
        v_fecha_nac := r.fecha_nac;

        v_nombre_comp := UPPER(v_nombre || ' ' || v_apellido);

        ------------------------------------------------------------------
        -- Años trabajados
        v_anios_trab :=
            TRUNC(MONTHS_BETWEEN(v_fecha_proc, v_fecha_ing) / 12);

        ------------------------------------------------------------------
        -- NOMBRE_USUARIO
        v_usuario :=
            LOWER(SUBSTR(v_estado,1,1)) ||
            UPPER(SUBSTR(v_nombre,1,3)) ||
            LENGTH(v_nombre) || '*' ||
            SUBSTR(v_run,-3) ||
            v_anios_trab;

        IF v_anios_trab < 10 THEN
            v_usuario := v_usuario || 'X';
        END IF;

        ------------------------------------------------------------------
        -- CLAVE_USUARIO
        v_clave :=
            SUBSTR(v_run,3,1) ||
            (EXTRACT(YEAR FROM v_fecha_nac) + 2) ||
            LPAD(MOD(v_sueldo,1000),3,'0');

        IF v_estado IN ('CASADO','ACUERDO') THEN
            v_clave := v_clave || LOWER(SUBSTR(v_apellido,1,2));
        ELSIF v_estado IN ('SOLTERO','DIVORCIADO') THEN
            v_clave := v_clave || LOWER(SUBSTR(v_apellido,1,1) || SUBSTR(v_apellido,-1));
        ELSIF v_estado = 'VIUDO' THEN
            v_clave := v_clave || LOWER(SUBSTR(v_apellido,-3,2));
        ELSIF v_estado = 'SEPARADO' THEN
            v_clave := v_clave || LOWER(SUBSTR(v_apellido,-2));
        END IF;

        v_clave :=
            v_clave || v_id_emp || TO_CHAR(v_fecha_proc,'MMYYYY');

        ------------------------------------------------------------------
        -- Inserción
        INSERT INTO usuario_clave
        VALUES (
            v_id_emp,
            v_run,
            v_dv,
            v_nombre_comp,
            v_usuario,
            v_clave
        );

        v_contador := v_contador + 1;
    END LOOP;

    ------------------------------------------------------------------
    -- Confirmación
    DBMS_OUTPUT.PUT_LINE('Total esperados: ' || v_total_emp);
    DBMS_OUTPUT.PUT_LINE('Total procesados: ' || v_contador);

    IF v_contador = v_total_emp THEN
        COMMIT;
        DBMS_OUTPUT.PUT_LINE('COMMIT ejecutado correctamente.');
    ELSE
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ROLLBACK ejecutado.');
    END IF;

END;
/

