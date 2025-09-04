-- Busca en esa partición (received_day = '2025-09-01') todos los archivos de datos que midan < 128MB.
-- Combínalos en archivos nuevos más grandes (hasta el tamaño de destino, que en tu tabla es 128–256MB).
-- Borra los ficheros pequeños y actualiza el snapshot de Iceberg.
-- Ayer (día cerrado)
ALTER TABLE nessie.telematics.gps_reports
  EXECUTE optimize(file_size_threshold => '256MB')
WHERE received_day = current_date - INTERVAL '1' day
  AND device_id_bucket BETWEEN 0 AND 63;

ALTER TABLE nessie.telematics.gps_reports
  EXECUTE optimize(file_size_threshold => '256MB')
WHERE received_day = current_date - INTERVAL '1' day
  AND device_id_bucket BETWEEN 64 AND 127;

-- Elimina los archivos huérfanos (orphan files) que no estén referenciados por ningún snapshot.
SET SESSION nessie.remove_orphan_files_min_retention = '0s';
ALTER TABLE nessie.telematics.gps_reports EXECUTE remove_orphan_files(retention_threshold => '0s');