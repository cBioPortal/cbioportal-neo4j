-- note, this script should be called in the following manner which set @outfile_dir:
-- mysql --user= --password= -e "set @outfile_dir = '/path-to-output-directory/'; source /path/export-cbioportal-from-mysql.sql;" dbname

-- dump gene data
-- all the dressing around the union, including the union all, limit 1, select * ... AS 'a' is to get the column names including in the outfile
SET @outfile = CONCAT(@outfile_dir, 'genes.csv');
SET @export_statement = CONCAT("select * from ((select 'EntrezGeneID', 'HugoGeneSymbol', 'GeneticEntityID', 'Type', 'Cytoband', 'Length' from gene limit 1) union all (select entrez_gene_id, hugo_gene_symbol, genetic_entity_id, type, cytoband, length from gene)) AS a into outfile '", @outfile, "' fields terminated by ',' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump cancer studies
-- all the dressing around the union, including the union all, limit 1, select * ... AS 'a' is to get the column names including in the outfile
SET @outfile = CONCAT(@outfile_dir, 'cancer-studies.csv');
SET @export_statement = CONCAT("select * from ((select 'CancerStudyID','CancerStudyIdentifier','TypeOfCancerID','Name','ShortName','Description','PubMedID','Citation' from cancer_study limit 1) union all (select cancer_study_id, cancer_study_identifier, type_of_cancer_id, name, short_name, description, pmid, citation from cancer_study)) as a into outfile '", @outfile, "' fields terminated by ',' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump clinical attribute meta
-- all the dressing around the union, including the union all, limit 1, select * ... AS 'a' is to get the column names including in the outfile
SET @outfile = CONCAT(@outfile_dir, 'clinical-attributes-meta.csv');
SET @export_statement = CONCAT("select * from ((select 'AttrID','DisplayName','CancerStudyID' from clinical_attribute_meta limit 1) union all (select attr_id, display_name, cancer_study_id from clinical_attribute_meta where attr_id in ('SEX','ONCOTREE_CODE','SAMPLE_TYPE'))) as a into outfile '", @outfile, "' fields terminated by ',' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump genetic profiles
-- all the dressing around the union, including the union all, limit 1, select * ... AS 'a' is to get the column names including in the outfile
SET @outfile = CONCAT(@outfile_dir, 'genetic-profiles.csv');
SET @export_statement = CONCAT("select * from ((select 'GeneticProfileID','StableID','CancerStudyID','GeneticAlterationType','DataType', 'Name', 'Description' from genetic_profile limit 1) union all (select genetic_profile_id, stable_id, cancer_study_id, genetic_alteration_type, datatype, name, description from genetic_profile)) as a into outfile '", @outfile, "' fields terminated by ',' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump patients
-- all the dressing around the union, including the union all, limit 1, select * ... AS 'a' is to get the column names including in the outfile
SET @outfile = CONCAT(@outfile_dir, 'patients.csv');
SET @export_statement = CONCAT("select * from ((select 'InternalID','StableID','CancerStudyID' from patient limit 1) union all (select internal_id, stable_id, cancer_study_id from patient)) as a into outfile '", @outfile, "' fields terminated by ',' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump clinical attribute patient data
-- all the dressing around the union, including the union all, limit 1, select * ... AS 'a' is to get the column names including in the outfile
SET @outfile = CONCAT(@outfile_dir, 'patients-clinical-attributes-data.csv');
SET @export_statement = CONCAT("select * from ((select 'InternalID', 'CancerStudyID', 'AttrID','AttrValue' from patient limit 1) union all (select patient.internal_id, cancer_study_Id, attr_id, attr_value from patient inner join clinical_patient on patient.internal_id = clinical_patient.internal_id where attr_id in ('SEX','ONCOTREE_CODE','SAMPLE_TYPE'))) as a into outfile '", @outfile, "' fields terminated by ',' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump samples
-- all the dressing around the union, including the union all, limit 1, select * ... AS 'a' is to get the column names including in the outfile
SET @outfile = CONCAT(@outfile_dir, 'samples.csv');
SET @export_statement = CONCAT("select * from ((select 'InternalID','StableID', 'SampleType', 'PatientID', 'CancerStudyID','TypeOfCancerID' from sample limit 1) union all (select sample.internal_id, sample.stable_id, sample_type, patient_id, patient.cancer_study_id, type_of_cancer_id from sample inner join patient on sample.patient_id = patient.internal_id)) as a into outfile '", @outfile, "' fields terminated by ',' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump clinical attribute sample data
-- all the dressing around the union, including the union all, limit 1, select * ... AS 'a' is to get the column names including in the outfile
SET @outfile = CONCAT(@outfile_dir, 'samples-clinical-attributes-data.csv');
SET @export_statement = CONCAT("select * from ((select 'InternalID', 'CancerStudyID', 'AttrID','AttrValue' from sample limit 1) union all (select sample.internal_id, cancer_study_Id, attr_id, attr_value from sample inner join clinical_sample on sample.internal_id = clinical_sample.internal_id inner join patient on sample.patient_id = patient.internal_id where attr_id in ('SEX','ONCOTREE_CODE','SAMPLE_TYPE'))) as a into outfile '", @outfile, "' fields terminated by ',' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump samples--genomic profiles - those in genetic_profile_samples table
-- all the dressing around the union, including the union all, limit 1, select * ... AS 'a' is to get the column names including in the outfile
SET @outfile = CONCAT(@outfile_dir, 'genetic-profile-samples.csv');
SET @export_statement = CONCAT("select * from ((select 'GeneticProfileID','InternalSampleID' from genetic_profile_data_expanded limit 1) union all (select genetic_profile_id, internal_sample_id from genetic_profile_data_expanded)) as a into outfile '", @outfile, "' fields terminated by ',' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump samples--genomic profiles - those in mutations table
-- all the dressing around the union, including the union all, limit 1, select * ... AS 'a' is to get the column names including in the outfile
SET @outfile = CONCAT(@outfile_dir, 'mutation-genetic-profile-samples.csv');
SET @export_statement = CONCAT("select * from ((select 'GeneticProfileID','InternalSampleID' from mutation limit 1) union all (select genetic_profile_id, sample_id from mutation)) as a into outfile '", @outfile, "' fields terminated by ',' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump sample - gene mutation data
-- all the dressing around the union, including the union all, limit 1, select * ... AS 'a' is to get the column names including in the outfile
SET @outfile = CONCAT(@outfile_dir, 'samples-mutations-genes.csv');
SET @export_statement = CONCAT("select * from ((select 'GeneticProfileID','InternalSampleID','EntrezGeneID','ProteinChange','MutationType' from mutation limit 1) union all (select genetic_profile_id, sample_id, mutation.entrez_gene_id, mutation_event.protein_change, mutation_event.mutation_type from mutation inner join mutation_event on mutation.mutation_event_id = mutation_event.mutation_event_id)) as a into outfile '", @outfile, "' fields terminated by ',' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump sample - gene genetic_alteration data
-- all the dressing around the union, including the union all, limit 1, select * ... AS 'a' is to get the column names including in the outfile
SET @outfile = CONCAT(@outfile_dir, 'samples-genetic-alterations-genes.csv');
SET @export_statement = CONCAT("select * from ((select 'GeneticProfileID','InternalSampleID','EntrezGeneID','Alteration' from genetic_profile_data_expanded limit 1) union all (select genetic_profile_id, internal_sample_id, entrez_gene_id, alteration from genetic_profile_data_expanded inner join gene on genetic_profile_data_expanded.genetic_entity_id = gene.genetic_entity_id)) as a into outfile '", @outfile, "' fields terminated by ',' lines terminated by '\n'");
PREPARE prepared_export_statement from @export_statement;
EXECUTE prepared_export_statement;
DEALLOCATE PREPARE prepared_export_statement;

-- dump sample-cna*-genes.csv files (the foreach trick to makeup for conditionals takes way too long in cypher so we bin the cna data here to create specific relationships in neo4j)
DROP PROCEDURE IF EXISTS `create_cna_data_files_by_discrete_value`;
DELIMITER $$
CREATE PROCEDURE `create_cna_data_files_by_discrete_value`(outfiles TEXT, alterations TEXT)
BEGIN
    DECLARE loop_counter INT;
    SET loop_counter = 0;
    WHILE loop_counter < 5 DO
          SET @csvfile = SUBSTRING_INDEX(outfiles, ',', 1);
          SET @outfile = CONCAT(@outfile_dir, @csvfile);
          SET @alteration =  SUBSTRING_INDEX(alterations, ',', 1);
          SET @export_statement = CONCAT("select * from ((select 'GeneticProfileID','InternalSampleID','EntrezGeneID','Alteration' from genetic_profile_data_expanded limit 1) union all (select genetic_profile_data_expanded.genetic_profile_id, internal_sample_id, entrez_gene_id, alteration from genetic_profile_data_expanded inner join genetic_profile on genetic_profile_data_expanded.genetic_profile_id = genetic_profile.genetic_profile_id inner join gene on genetic_profile_data_expanded.genetic_entity_id = gene.genetic_entity_id where genetic_alteration_type = 'COPY_NUMBER_ALTERATION' and alteration = @alteration)) as a into outfile '", @outfile, "' fields terminated by ',' lines terminated by '\n'");
          PREPARE prepared_export_statement from @export_statement;
          EXECUTE prepared_export_statement;
          DEALLOCATE PREPARE prepared_export_statement;
          -- following + 1 + 1 is length of entry plus the comma then move to the next position
          SET outfiles = SUBSTRING(outfiles FROM CHAR_LENGTH(@csvfile) + 1 + 1);
          SET alterations = SUBSTRING(alterations FROM CHAR_LENGTH(@alteration) + 1 + 1);
          SET loop_counter = loop_counter + 1;
    END WHILE;
END $$
DELIMITER ;
CALL create_cna_data_files_by_discrete_value('samples-cna-homdel-genes.csv,samples-cna-hetloss-genes.csv,samples-cna-diploid-genes.csv,samples-cna-gain-genes.csv,samples-cna-amp-genes.csv', '-2,-1,0,1,2');
DROP PROCEDURE IF EXISTS `create_cna_data_files_by_discrete_value`;
