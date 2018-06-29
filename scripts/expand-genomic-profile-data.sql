--------------------------------------------------------------------------------
--- routines to expand data in genetic_profile_samples - genetic
--------------------------------------------------------------------------------

--
-- create the table to contain the expanded alteration data
--
DROP TABLE IF EXISTS `genetic_profile_data_expanded`;
CREATE TABLE `genetic_profile_data_expanded` (
   `GENETIC_PROFILE_ID` int(11) NOT NULL,
   `INTERNAL_SAMPLE_ID` int(11) NOT NULL,
   `GENETIC_ENTITY_ID` int(11) NOT NULL,
   `ALTERATION` int(11) NOT NULL,
   PRIMARY KEY (`GENETIC_PROFILE_ID`, `INTERNAL_SAMPLE_ID`, `GENETIC_ENTITY_ID`)
);

--
-- computes the number of records in a delimited list - the delimiter should be paramaterized
--
DROP PROCEDURE IF EXISTS `length_of_delimited_list`;
DELIMITER $$
CREATE PROCEDURE `length_of_delimited_list`(delimited_list LONGTEXT, OUT delimited_list_length INT)
BEGIN
    SET delimited_list_length = LENGTH(TRIM(BOTH ',' FROM delimited_list)) - LENGTH(REPLACE(TRIM(BOTH ',' FROM delimited_list), ',', ''));
END $$
DELIMITER ;

--
-- creates a genetic_profile_id, hugo symbol, stable sample id, alteration value in the given table
--
DROP PROCEDURE IF EXISTS `insert_genetic_profile_data_expanded`;
DELIMITER $$
CREATE PROCEDURE `insert_genetic_profile_data_expanded`(genetic_profile_id INT, internal_sample_id INT, genetic_entity_id INT, genetic_alteration_value INT, expanded_table_name TEXT)
BEGIN
    SET @insert_statement = CONCAT('insert into ', expanded_table_name, ' values (', genetic_profile_id, ',"', internal_sample_id, '","', genetic_entity_id, '","', genetic_alteration_value, '")');
    PREPARE insert_statement from @insert_statement;
    EXECUTE insert_statement;
    DEALLOCATE PREPARE insert_statement;
END $$
DELIMITER ;

--
-- processes a single genetic_profile_samples/genetic_alteration record pair
-- 
DROP PROCEDURE IF EXISTS `process_genetic_profile_samples_and_alteration_data`;
DELIMITER $$
CREATE PROCEDURE `process_genetic_profile_samples_and_alteration_data`(genetic_profile_id INT, genetic_profile_sample_ids LONGTEXT, genetic_entity_id INT, genetic_alteration_values LONGTEXT, sample_ids_length INT, expanded_table_name TEXT)
BEGIN
    DECLARE loop_counter INT;
    DECLARE internal_sample_id TEXT;
    DECLARE genetic_alteration_value TEXT;
    SET loop_counter = 0;
    WHILE loop_counter < sample_ids_length DO
       SET internal_sample_id = SUBSTRING_INDEX(genetic_profile_sample_ids, ',', 1);
       SET genetic_alteration_value = SUBSTRING_INDEX(genetic_alteration_values, ',', 1);
       CALL insert_genetic_profile_data_expanded(genetic_profile_id, internal_sample_id, genetic_entity_id, genetic_alteration_value, expanded_table_name);
       -- following + 1 + 1 is length of sample id or alteration value plus the comma then move to the next position
       set genetic_profile_sample_ids = SUBSTRING(genetic_profile_sample_ids FROM CHAR_LENGTH(internal_sample_id) + 1 + 1);
       set genetic_alteration_values = SUBSTRING(genetic_alteration_values FROM CHAR_LENGTH(genetic_alteration_value) + 1 + 1);
       SET loop_counter = loop_counter + 1;
    END WHILE;

END $$
DELIMITER ;

--
-- process all data in a  single genetic_profile
-- 
DROP PROCEDURE IF EXISTS `expand_genetic_profile_data`;
DELIMITER $$
CREATE PROCEDURE `expand_genetic_profile_data`(genetic_profile_id INT, expanded_table_name TEXT)
BEGIN
    DECLARE loop_counter INT;
    DECLARE sample_ids_length INT;
    DECLARE genetic_alteration_values_length INT;
    SET @genetic_profile_sample_ids_statement = CONCAT('select @genetic_profile_sample_ids := ordered_sample_list from genetic_profile_samples where genetic_profile_id = ', genetic_profile_id);
    PREPARE prepared_genetic_profile_sample_ids_statement from @genetic_profile_sample_ids_statement;
    EXECUTE prepared_genetic_profile_sample_ids_statement;
    CALL length_of_delimited_list(@genetic_profile_sample_ids, sample_ids_length);
    -- get number of genetic entity/alteration records for the given genetic profile id
    SET @num_genetic_entity_alteration_value_pairs_statement = CONCAT('select @num_genetic_entity_alteration_value_pairs := count(*) from genetic_alteration where genetic_profile_id = ', genetic_profile_id);
    PREPARE prepared_num_genetic_entity_alteration_value_pairs_statement from @num_genetic_entity_alteration_value_pairs_statement;
    EXECUTE prepared_num_genetic_entity_alteration_value_pairs_statement;
    -- loop each pair
    SET loop_counter = 0;
    WHILE loop_counter < @num_genetic_entity_alteration_value_pairs DO
       -- get genetic entity/alteration values
       SET @genetic_entity_alteration_values_pair_statement = CONCAT('select @genetic_entity_id := genetic_entity_id, @genetic_alteration_values := `values` from genetic_alteration where genetic_profile_id = ', genetic_profile_id, ' LIMIT ', loop_counter, ', 1');
       PREPARE prepared_genetic_entity_alteration_values_pair_statement from @genetic_entity_alteration_values_pair_statement;
       EXECUTE prepared_genetic_entity_alteration_values_pair_statement;
       -- sanity check string lengths before processing
       CALL length_of_delimited_list(@genetic_alteration_values, genetic_alteration_values_length);
       IF (sample_ids_length = genetic_alteration_values_length) THEN
          CALL process_genetic_profile_samples_and_alteration_data(genetic_profile_id, @genetic_profile_sample_ids, @genetic_entity_id, @genetic_alteration_values, sample_ids_length, expanded_table_name);
       END IF;
       DEALLOCATE PREPARE prepared_genetic_entity_alteration_values_pair_statement;
       SET loop_counter = loop_counter + 1;
    END WHILE;
    -- deallocate statements
    DEALLOCATE PREPARE prepared_genetic_profile_sample_ids_statement;
    DEALLOCATE PREPARE prepared_num_genetic_entity_alteration_value_pairs_statement;
END $$
DELIMITER ;

--
-- the driver procedure
-- 
DROP PROCEDURE IF EXISTS `expand_all_genetic_profiles`;
DELIMITER $$
CREATE PROCEDURE `expand_all_genetic_profiles`(expanded_table_name TEXT)
BEGIN
    DECLARE loop_counter INT;
    -- get number of profiles in table
    PREPARE prepared_num_genetic_profiles from 'SELECT COUNT(*) from genetic_profile INTO @num_genetic_profiles';
    EXECUTE prepared_num_genetic_profiles;
    SET loop_counter = 0;
    -- process all profiles except mutation/fusion
    WHILE loop_counter < @num_genetic_profiles DO
       SET @genetic_profile_statement = CONCAT('select @genetic_profile_id := genetic_profile_id, @alteration_type := genetic_alteration_type from genetic_profile LIMIT ', loop_counter, ', 1');
       PREPARE prepared_genetic_profile_statement from @genetic_profile_statement;
       EXECUTE prepared_genetic_profile_statement;
       IF (STRCMP(@alteration_type, 'MUTATION_EXTENDED') != 0 AND STRCMP(@alteration_type, 'FUSION') != 0) THEN
           CALL expand_genetic_profile_data(@genetic_profile_id, expanded_table_name);
       END IF;
       DEALLOCATE PREPARE prepared_genetic_profile_statement;
       SET loop_counter = loop_counter + 1;
    END WHILE;
    DEALLOCATE PREPARE prepared_num_genetic_profiles;
END $$
DELIMITER ;

CALL expand_all_genetic_profiles('genetic_profile_data_expanded');

-- cleanup temp table/procedures
DROP TABLE IF EXISTS `genetic_profile_data_expanded`;
DROP PROCEDURE IF EXISTS `length_of_delimited_list`;
DROP PROCEDURE IF EXISTS `insert_genetic_profile_data_expanded`;
DROP PROCEDURE IF EXISTS `process_genetic_profile_samples_and_alteration_data`;
DROP PROCEDURE IF EXISTS `expand_genetic_profile_data`;
DROP PROCEDURE IF EXISTS `expand_all_genetic_profiles`;
