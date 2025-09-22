USE nessie.telematics;

ALTER TABLE nessie.telematics.telematics_real_time EXECUTE optimize(file_size_threshold => '256MB');

-- SI NO FUNCIONA
--WHERE received_day = current_date - INTERVAL '1' day
--  AND device_id_bucket BETWEEN 0 AND 63;
-- o
-- AND device_id_bucket BETWEEN 64 AND 127;

ANALYZE nessie.telematics.telematics_real_time;