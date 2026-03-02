
-- TRIGGER
CREATE OR REPLACE TRIGGER trg_total_consumos
AFTER INSERT OR UPDATE OR DELETE ON consumo
FOR EACH ROW
DECLARE
BEGIN
   -- INSERT
   IF INSERTING THEN
      UPDATE total_consumos
      SET monto_consumos = monto_consumos + :NEW.monto
      WHERE id_huesped = :NEW.id_huesped;

      IF SQL%ROWCOUNT = 0 THEN
         INSERT INTO total_consumos(id_huesped, monto_consumos)
         VALUES (:NEW.id_huesped, :NEW.monto);
      END IF;
   END IF;

   -- UPDATE
   IF UPDATING THEN
      UPDATE total_consumos
      SET monto_consumos = monto_consumos 
                           + (:NEW.monto - :OLD.monto)
      WHERE id_huesped = :NEW.id_huesped;
   END IF;

   -- DELETE
   IF DELETING THEN
      UPDATE total_consumos
      SET monto_consumos = monto_consumos - :OLD.monto
      WHERE id_huesped = :OLD.id_huesped;
   END IF;
END;
/

-- PACKAGE
CREATE OR REPLACE PACKAGE pkg_tours IS
   FUNCTION fn_total_tours(p_id_huesped NUMBER) RETURN NUMBER;
   v_total_tours NUMBER;
END pkg_tours;
/

CREATE OR REPLACE PACKAGE BODY pkg_tours IS

   FUNCTION fn_total_tours(p_id_huesped NUMBER) RETURN NUMBER IS
      v_total NUMBER := 0;
   BEGIN
      SELECT NVL(SUM(t.valor_tour * ht.num_personas),0)
      INTO v_total
      FROM huesped_tour ht
      JOIN tour t ON ht.id_tour = t.id_tour
      WHERE ht.id_huesped = p_id_huesped;

      v_total_tours := v_total;
      RETURN v_total;

   EXCEPTION
      WHEN NO_DATA_FOUND THEN
         RETURN 0;
   END;

END pkg_tours;
/


-- FUNCIONES ALMACENADAS
CREATE OR REPLACE FUNCTION fn_agencia(p_id_huesped NUMBER)
RETURN VARCHAR2
IS
   v_agencia VARCHAR2(100);
   v_error   VARCHAR2(300);
BEGIN

   SELECT a.nom_agencia
   INTO v_agencia
   FROM huesped h
   LEFT JOIN agencia a 
      ON h.id_agencia = a.id_agencia
   WHERE h.id_huesped = p_id_huesped;

   IF v_agencia IS NULL THEN
      RETURN 'NO REGISTRA AGENCIA';
   END IF;

   RETURN v_agencia;

EXCEPTION
   WHEN OTHERS THEN
      v_error := SQLERRM;

      INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
      VALUES (
         sq_error.NEXTVAL,
         'FN_AGENCIA - HUESPED ' || p_id_huesped,
         v_error
      );

      RETURN 'NO REGISTRA AGENCIA';
END;
/


CREATE OR REPLACE FUNCTION fn_consumos(p_id_reserva NUMBER)
RETURN NUMBER
IS
   v_total NUMBER := 0;
   v_error VARCHAR2(300);
BEGIN

   SELECT NVL(SUM(monto),0)
   INTO v_total
   FROM consumo
   WHERE id_reserva = p_id_reserva;

   RETURN v_total;

EXCEPTION
   WHEN OTHERS THEN
      v_error := SQLERRM;

      INSERT INTO reg_errores (id_error, nomsubprograma, msg_error)
      VALUES (
         sq_error.NEXTVAL,
         'FN_CONSUMOS - RESERVA ' || p_id_reserva,
         v_error
      );

      RETURN 0;
END;
/


-- PROCEDIMIENTO ALMACENADO
CREATE OR REPLACE PROCEDURE pr_calculo_cobranza(
   p_fecha_proceso DATE,
   p_id_huesped    NUMBER
)
IS

   CURSOR c_reservas IS
      SELECT *
      FROM reserva
      WHERE id_huesped = p_id_huesped
      AND ingreso <= p_fecha_proceso;

   v_nombre              VARCHAR2(100);
   v_agencia             VARCHAR2(40);
   v_alojamiento         NUMBER := 0;
   v_consumos            NUMBER := 0;
   v_tours               NUMBER := 0;
   v_subtotal            NUMBER := 0;
   v_descuento_consumos  NUMBER := 0;
   v_descuento_agencia   NUMBER := 0;
   v_total               NUMBER := 0;

BEGIN

   DELETE FROM detalle_diario_huespedes;
   COMMIT;

   FOR rec IN c_reservas LOOP

      -- Reiniciar variables
      v_alojamiento        := 0;
      v_consumos           := 0;
      v_tours              := 0;
      v_subtotal           := 0;
      v_descuento_consumos := 0;
      v_descuento_agencia  := 0;
      v_total              := 0;

      -- Nombre huésped
      SELECT nom_huesped || ' ' || appat_huesped || ' ' || apmat_huesped
      INTO v_nombre
      FROM huesped
      WHERE id_huesped = rec.id_huesped;

      -- Agencia
      v_agencia := fn_agencia(rec.id_huesped);

      -- Alojamiento
      SELECT NVL(SUM((h.valor_habitacion + h.valor_minibar) * rec.estadia),0)
      INTO v_alojamiento
      FROM detalle_reserva dr
      JOIN habitacion h 
         ON dr.id_habitacion = h.id_habitacion
      WHERE dr.id_reserva = rec.id_reserva;

      -- Consumos
      v_consumos := fn_consumos(rec.id_reserva);

      -- Tours (tu modelo no relaciona tours con reserva, así que queda 0)
      v_tours := 0;

      -- Subtotal
      v_subtotal := v_alojamiento + v_consumos + v_tours;

      -- Descuento consumos (ejemplo 10% si supera 100)
      IF v_consumos > 100 THEN
         v_descuento_consumos := v_consumos * 0.10;
      END IF;

      -- Descuento agencia (5% si tiene agencia)
      IF v_agencia <> 'NO REGISTRA AGENCIA' THEN
         v_descuento_agencia := v_subtotal * 0.05;
      END IF;

      -- Total final
      v_total := v_subtotal
                 - v_descuento_consumos
                 - v_descuento_agencia;

      -- Insert final
      INSERT INTO detalle_diario_huespedes(
         id_huesped,
         nombre,
         agencia,
         alojamiento,
         consumos,
         tours,
         subtotal_pago,
         descuento_consumos,
         descuentos_agencia,
         total
      )
      VALUES (
         rec.id_huesped,
         v_nombre,
         v_agencia,
         v_alojamiento,
         v_consumos,
         v_tours,
         v_subtotal,
         v_descuento_consumos,
         v_descuento_agencia,
         v_total
      );

   END LOOP;

   COMMIT;

END;
/

--PRUEBA
BEGIN
   pr_calculo_cobranza(
      TO_DATE('18/08/2021','DD/MM/YYYY'),
      915
   );
END;
/

BEGIN
   pr_calculo_cobranza(
      TO_DATE('18/08/2021','DD/MM/YYYY'),
      915
   );
END;
/
