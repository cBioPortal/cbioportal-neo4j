-- note, this script should be called in the following manner which set @outfile_dir:
-- mysql --user= --password= -e "set @outfile_dir = '/path-to-output-directory/'; source /path/neo4j-admin-export-cbioportal-from-mysql.sql;" dbname

-- 
-- routines to expand data in genetic_profile_samples - genetic
-- 

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
    END WHILE;
    DEALLOCATE PREPARE prepared_num_genetic_profiles;
END $$
DELIMITER ;

--
-- dump graph nodes
--

-- dump type_of_cancer nodes
SET @outfile = CONCAT(@outfile_dir, 'type-of-cancer-header.csv');
SET @export_statement = CONCAT("select 'type_of_cancer_id:id(type-of-cancer-id)', 'name', 'clinical_trial_keywords', 'dedicated_color', 'short_name', 'parent', ':label' from type_of_cancer limit 1 into outfile'", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;
SET @outfile = CONCAT(@outfile_dir, 'type-of-cancer.csv');
SET @export_statement = CONCAT("select type_of_cancer_id, name, clinical_trial_keywords, dedicated_color, short_name, parent, 'type_of_cancer' from type_of_cancer into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump gene nodes
SET @outfile = CONCAT(@outfile_dir, 'gene-header.csv');
SET @export_statement = CONCAT("select 'entrez_gene_id:id(gene-id)', 'hugo_gene_symbol', 'genetic_entity_id:int', 'type', 'cytoband', 'length:int', ':label' from gene limit 1 into outfile'", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;
SET @outfile = CONCAT(@outfile_dir, 'gene.csv');
SET @export_statement = CONCAT("select entrez_gene_id, hugo_gene_symbol, genetic_entity_id, type, cytoband, length, LOWER(genetic_entity.entity_type) from gene inner join genetic_entity on genetic_entity_id = genetic_entity.id into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump clinical_attribute_meta nodes
SET @outfile = CONCAT(@outfile_dir, 'clinical-attribute-meta-header.csv');
SET @export_statement = CONCAT("select 'attr_id:id(clinical-attribute-meta-id)', 'display_name', 'description', 'datatype', ':label' from clinical_attribute_meta limit 1 into outfile'", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;
SET @outfile = CONCAT(@outfile_dir, 'clinical-attribute-meta.csv');
SET @export_statement = CONCAT("select attr_id, display_name, description, datatype, 'clinical_attr_meta' from clinical_attribute_meta where attr_id in ('sex', 'oncotree_code', 'sample_type') into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump cancer_study nodes
SET @outfile = CONCAT(@outfile_dir, 'cancer-study-header.csv');
SET @export_statement = CONCAT("select 'cancer_study_id:id(cancer-study-id)', 'cancer_study_identifier', 'name', 'short_name', 'description', 'pmid', 'citation', ':label' from cancer_study limit 1 into outfile'", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;
SET @outfile = CONCAT(@outfile_dir, 'cancer-study.csv');
SET @export_statement = CONCAT("select cancer_study_id, cancer_study_identifier, name, short_name, description, pmid, citation, 'cancer_study' from cancer_study into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump genetic_profile nodes
SET @outfile = CONCAT(@outfile_dir, 'genetic-profile-header.csv');
SET @export_statement = CONCAT("select 'genetic_profile_id:id(genetic-profile-id)', 'stable_id', 'genetic_alteration_type', 'datatype', 'name', 'description', ':label' from genetic_profile limit 1 into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;
SET @outfile = CONCAT(@outfile_dir, 'genetic-profile.csv');
SET @export_statement = CONCAT("select genetic_profile_id, stable_id, genetic_alteration_type, datatype, name, description, 'genetic_profile' from genetic_profile into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump patient nodes
SET @outfile = CONCAT(@outfile_dir, 'patient-header.csv');
SET @export_statement = CONCAT("select 'internal_id:id(patient-id)', 'stable_id', ':label' from patient limit 1 into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;
SET @outfile = CONCAT(@outfile_dir, 'patient.csv');
SET @export_statement = CONCAT("select internal_id, stable_id, 'patient' from patient into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump sample nodes
SET @outfile = CONCAT(@outfile_dir, 'sample-header.csv');
SET @export_statement = CONCAT("select 'internal_id:id(sample-id)', 'stable_id', ':label' from sample limit 1 into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;
SET @outfile = CONCAT(@outfile_dir, 'sample.csv');
SET @export_statement = CONCAT("select internal_id, stable_id, 'sample' from sample into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

--
-- dump graph edges
--

-- dump cancer_study->type_of_cancer relationships
SET @outfile = CONCAT(@outfile_dir, 'cancer-study--type-of-cancer-header.csv');
SET @export_statement = CONCAT("select ':start_id(cancer-study-id)', ':end_id(type-of-cancer-id)', ':type' from cancer_study limit 1 into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;
SET @outfile = CONCAT(@outfile_dir, 'cancer-study--type-of-cancer.csv');
SET @export_statement = CONCAT("select cancer_study_id, type_of_cancer_id, 'is_type_of_cancer' from cancer_study into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump cancer_study->clinical_attribute_meta relationships
SET @outfile = CONCAT(@outfile_dir, 'cancer-study--clinical-attribute-meta-header.csv');
SET @export_statement = CONCAT("select ':start_id(cancer-study-id)', ':end_id(clinical-attribute-meta-id)', ':type' from clinical_attribute_meta limit 1 into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;
SET @outfile = CONCAT(@outfile_dir, 'cancer-study--clinical-attribute-meta.csv');
SET @export_statement = CONCAT("select cancer_study_id, attr_id, 'includes_clinical_attribute' from clinical_attribute_meta where attr_id in ('sex', 'oncotree_code', 'sample_type') into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump cancer_study->patient relationships
SET @outfile = CONCAT(@outfile_dir, 'cancer-study--patient-header.csv');
SET @export_statement = CONCAT("select ':start_id(cancer-study-id)', ':end_id(patient-id)', ':type' from patient limit 1 into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;
SET @outfile = CONCAT(@outfile_dir, 'cancer-study--patient.csv');
SET @export_statement = CONCAT("select cancer_study_id, internal_id, 'includes_patient' from patient into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump cancer_study->sample relationships
SET @outfile = CONCAT(@outfile_dir, 'cancer-study--sample-header.csv');
SET @export_statement = CONCAT("select ':start_id(cancer-study-id)', ':end_id(sample-id)', ':type' from sample limit 1 into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;
SET @outfile = CONCAT(@outfile_dir, 'cancer-study--sample.csv');
SET @export_statement = CONCAT("select patient.cancer_study_id, sample.internal_id, 'includes_sample' from sample inner join patient on patient_id = patient.internal_id into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump patient->sample relationships
SET @outfile = CONCAT(@outfile_dir, 'patient--sample-header.csv');
SET @export_statement = CONCAT("select ':start_id(patient-id)', ':end_id(sample-id)', ':type' from sample limit 1 into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;
SET @outfile = CONCAT(@outfile_dir, 'patient--sample.csv');
SET @export_statement = CONCAT("select patient_id, internal_id, 'with_sample' from sample into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump patient->clinical-attribute relationships
SET @outfile = CONCAT(@outfile_dir, 'patient--clinical-header.csv');
SET @export_statement = CONCAT("select ':start_id(patient-id)', 'value', ':end_id(clinical-attribute-meta-id)', ':type' from clinical_patient limit 1 into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;
SET @outfile = CONCAT(@outfile_dir, 'patient--clinical.csv');
SET @export_statement = CONCAT("select internal_id, attr_value, attr_id, 'has_attribute_value' from clinical_patient where attr_id in ('sex') into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump sample->clinical-attribute relationships
SET @outfile = CONCAT(@outfile_dir, 'sample--clinical-header.csv');
SET @export_statement = CONCAT("select ':start_id(sample-id)', 'value', ':end_id(clinical-attribute-meta-id)', ':type' from clinical_sample limit 1 into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;
SET @outfile = CONCAT(@outfile_dir, 'sample--clinical.csv');
SET @export_statement = CONCAT("select internal_id, attr_value, attr_id, 'has_attribute_value' from clinical_sample where attr_id in ('oncotree_code', 'sample_type') into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump sample->mutation relationships
SET @outfile = CONCAT(@outfile_dir, 'sample--mutation-header.csv');
SET @export_statement = CONCAT("select ':start_id(sample-id)', 'protein_change', 'mutation_type', ':end_id(gene-id)', ':type' from mutation limit 1 into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;
SET @outfile = CONCAT(@outfile_dir, 'sample--mutation.csv');
SET @export_statement = CONCAT("select sample_id, protein_change, mutation_type, mutation.entrez_gene_id, genetic_profile.stable_id from mutation inner join mutation_event on mutation.mutation_event_id = mutation_event.mutation_event_id inner join genetic_profile on mutation.genetic_profile_id = genetic_profile.genetic_profile_id into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump sample->genetic_alteration relationships
CALL expand_all_genetic_profiles('genetic_profile_data_expanded');

SET @outfile = CONCAT(@outfile_dir, 'sample--genetic-alteration-header.csv');
SET @export_statement = CONCAT("select ':start_id(sample-id)', 'alteration:int', ':end_id(gene-id)', ':type' from genetic_alteration limit 1 into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;
SET @outfile = CONCAT(@outfile_dir, 'sample--genetic-alteration.csv');
SET @export_statement = CONCAT("select internal_sample_id, alteration, gene.entrez_gene_id, genetic_profile.stable_id from genetic_profile_data_expanded inner join genetic_profile on genetic_profile_data_expanded.genetic_profile_id = genetic_profile.genetic_profile_id inner join gene where genetic_profile_data_expanded.genetic_entity_id = gene.genetic_entity_id into outfile '", @outfile, "' fields terminated by '\t' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- cleanup temp table/procedures
DROP TABLE IF EXISTS `genetic_profile_data_expanded`;
DROP PROCEDURE IF EXISTS `length_of_delimited_list`;
DROP PROCEDURE IF EXISTS `insert_genetic_profile_data_expanded`;
DROP PROCEDURE IF EXISTS `process_genetic_profile_samples_and_alteration_data`;
DROP PROCEDURE IF EXISTS `expand_genetic_profile_data`;
DROP PROCEDURE IF EXISTS `expand_all_genetic_profiles`;
