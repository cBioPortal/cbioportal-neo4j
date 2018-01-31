// create gene nodes
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///genes.csv" AS row
CREATE (:Gene {entrezGeneID: row.EntrezGeneID, hugoGeneSymbol: row.HugoGeneSymbol});

// studies
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///cancer-studies.csv" AS row
CREATE (:CancerStudy {cancerStudyID: row.CancerStudyID, cancerStudyIdentifier: row.CancerStudyIdentifier, typeOfCancer: row.TypeOfCancerID, name: row.Name, shortName: row.ShortName, description: row.Description, pubMedID: row.PubMedID, citation: row.Citation});

// clinical attribute metadata
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///clinical-attributes-meta.csv" AS row
CREATE (:ClinicalAttribute {attrID: row.AttrID, displayName: row.DisplayName});

// genetic profiles - call them molecular profiles in graph
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///genetic-profiles.csv" AS row
CREATE (:MolecularProfile {molecularProfileID: row.GeneticProfileID, stableID: row.StableID, alterationType: row.GeneticAlterationType, datatype: row.DataType, name: row.Name, description: row.Description});
 
// patients
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///patients.csv" AS row
CREATE (:Patient {internalID: row.InternalID, stableID: row.StableID});
 
// samples
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///samples.csv" AS row
CREATE (:Sample {internalID: row.InternalID, stableID: row.StableID, sampleType: row.SampleType, typeOfCancer: row.TypeOfCancerID});
 
CREATE INDEX ON :Gene(hugoGeneSymbol);
CREATE INDEX ON :Gene(entrezGeneID);
CREATE INDEX ON :CancerStudy(cancerStudyID);
CREATE INDEX ON :CancerStudy(cancerStudyIdentifier);
CREATE INDEX ON :ClinicalAttribute(attrID);
CREATE INDEX ON :MolecularProfile(molecularProfileID);
CREATE INDEX ON :MolecularProfile(stableID);
CREATE INDEX ON :Patient(internalID);
CREATE INDEX ON :Patient(stableID);
CREATE INDEX ON :Sample(internalID);
CREATE INDEX ON :Sample(stableID);

// ink clinical attributes with cancer studies
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///clinical-attributes-meta.csv" AS row
MATCH (clinicalAttribute:ClinicalAttribute { attrID: row.AttrID})
MATCH (cancerStudy:CancerStudy { cancerStudyID: row.CancerStudyID})
MERGE (clinicalAttribute)-[:CANCER_STUDY]->(cancerStudy);
 
// link molecular profile with cancer studies
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///genetic-profiles.csv" AS row
MATCH (molecularProfile:MolecularProfile { molecularProfileID: row.GeneticProfileID})
MATCH (cancerStudy:CancerStudy { cancerStudyID: row.CancerStudyID})
MERGE (molecularProfile)-[:CANCER_STUDY]->(cancerStudy);
 
// link patients with cancer studies
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///patients.csv" AS row
MATCH (patient:Patient { internalID: row.InternalID})
MATCH (cancerStudy:CancerStudy { cancerStudyID: row.CancerStudyID})
MERGE (patient)-[:CANCER_STUDY]->(cancerStudy);

// link patients with clinical attributes
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///patients-clinical-attributes-data.csv" AS row
MATCH (patient:Patient { internalID: row.InternalID})
MATCH (clinicalAttribute:ClinicalAttribute { attrID: row.AttrID})
MERGE (patient)-[ca:CLINICAL_ATTRIBUTE]->(clinicalAttribute)
ON CREATE SET ca.cancerStudyID = row.CancerStudyID, ca.value = row.AttrValue;

// link samples with cancer studies
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///samples.csv" AS row
MATCH (sample:Sample { internalID: row.InternalID})
MATCH (cancerStudy:CancerStudy { cancerStudyID: row.CancerStudyID})
MERGE (sample)-[:CANCER_STUDY]->(cancerStudy);

// link samples with clinical attributes
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///samples-clinical-attributes-data.csv" AS row
MATCH (sample:Sample { internalID: row.InternalID})
MATCH (clinicalAttribute:ClinicalAttribute { attrID: row.AttrID})
MERGE (sample)-[ca:CLINICAL_ATTRIBUTE]->(clinicalAttribute)
ON CREATE SET ca.cancerStudyID = row.CancerStudyID, ca.value = row.AttrValue;
 
// link samples with molecular profiles (genetic_profiles_sample table)
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///genetic-profile-samples.csv" AS row
MATCH (molecularProfile:MolecularProfile { molecularProfileID: row.GeneticProfileID})
MATCH (sample:Sample { internalID: row.InternalSampleID})
MERGE (sample)-[mp:MOLECULAR_PROFILE]->(molecularProfile)
ON CREATE SET mp.molecularProfileID = row.GeneticProfileID;
 
// link samples with molecular profiles (mutation table)
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///mutation-genetic-profile-samples.csv" AS row
MATCH (molecularProfile:MolecularProfile { molecularProfileID: row.GeneticProfileID})
MATCH (sample:Sample { internalID: row.InternalSampleID})
MERGE (sample)-[mp:MOLECULAR_PROFILE]->(molecularProfile)
ON CREATE SET mp.molecularProfileID = row.GeneticProfileID;
 
// link samples with patients
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///samples.csv" AS row
MATCH (sample:Sample { internalID: row.InternalID})
MATCH (patient:Patient { internalID: row.PatientID})
MERGE (sample)-[:PATIENT]->(patient);

// link samples-genes mutation data
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///samples-mutations-genes.csv" AS row
MATCH (sample:Sample { internalID: row.InternalSampleID})
MATCH (gene:Gene {entrezGeneID: row.EntrezGeneID})
MERGE (sample)-[m:MUTATION]->(gene)
ON CREATE SET m.molecularProfileID = row.GeneticProfileID, m.proteinChange = row.ProteinChange, m.mutationType = row.MutationType;

// link samples-genes genetic alteration data
USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///samples-genetic-alterations-genes.csv" AS row
MATCH (sample:Sample { internalID: row.InternalSampleID})
MATCH (gene:Gene {entrezGeneID: row.EntrezGeneID})
MERGE (sample)-[ma:MOLECULAR_ALTERATION]->(gene)
ON CREATE SET ma.molecularProfileID = row.GeneticProfileID, ma.alteration = row.Alteration;

// nice in theory but seems to crash with the volume of data - maybe revisit
// USING PERIODIC COMMIT
// LOAD CSV WITH HEADERS FROM "file:///samples-genetic-alterations-genes.csv" AS row
// MATCH (sample:Sample { internalID: row.InternalSampleID})
// MATCH (gene:Gene {entrezGeneID: row.EntrezGeneID})
// FOREACH(unused IN CASE WHEN row.Alteration IN ["-2"] THEN [1] else [] END |
//           MERGE (sample)-[ma:CNA_HOMDEL]->(gene)
//           ON CREATE SET ma.molecularProfileID = row.GeneticProfileID
// )
// FOREACH(unused IN CASE WHEN row.Alteration IN ["-1"] THEN [1] else [] END |
//           MERGE (sample)-[ma:CNA_HETLOSS]->(gene)
//           ON CREATE SET ma.molecularProfileID = row.GeneticProfileID
// )
// FOREACH(unused IN CASE WHEN row.Alteration IN ["0"] THEN [1] else [] END |
//           MERGE (sample)-[ma:CNA_DIPLOID]->(gene)
//           ON CREATE SET ma.molecularProfileID = row.GeneticProfileID
// )
// FOREACH(unused IN CASE WHEN row.Alteration IN ["1"] THEN [1] else [] END |
//           MERGE (sample)-[ma:CNA_GAIN]->(gene)
//           ON CREATE SET ma.molecularProfileID = row.GeneticProfileID
// )
// FOREACH(unused IN CASE WHEN row.Alteration IN ["2"] THEN [1] else [] END |
//           MERGE (sample)-[ma:CNA_AMP]->(gene)
//           ON CREATE SET ma.molecularProfileID = row.GeneticProfileID
// )
// ;

USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///samples-cna-homdel-genes.csv" AS row
MATCH (sample:Sample { internalID: row.InternalSampleID})
MATCH (gene:Gene {entrezGeneID: row.EntrezGeneID})
MERGE (sample)-[ma:CNA_HOMDEL]->(gene)
ON CREATE SET ma.molecularProfileID = row.GeneticProfileID;

USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///samples-cna-hetloss-genes.csv" AS row
MATCH (sample:Sample { internalID: row.InternalSampleID})
MATCH (gene:Gene {entrezGeneID: row.EntrezGeneID})
MERGE (sample)-[ma:CNA_HETLOSS]->(gene)
ON CREATE SET ma.molecularProfileID = row.GeneticProfileID;

USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///samples-cna-diploid-genes.csv" AS row
MATCH (sample:Sample { internalID: row.InternalSampleID})
MATCH (gene:Gene {entrezGeneID: row.EntrezGeneID})
MERGE (sample)-[ma:CNA_DIPLOID]->(gene)
ON CREATE SET ma.molecularProfileID = row.GeneticProfileID;

USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///samples-cna-gain-genes.csv" AS row
MATCH (sample:Sample { internalID: row.InternalSampleID})
MATCH (gene:Gene {entrezGeneID: row.EntrezGeneID})
MERGE (sample)-[ma:CNA_GAIN]->(gene)
ON CREATE SET ma.molecularProfileID = row.GeneticProfileID;

USING PERIODIC COMMIT
LOAD CSV WITH HEADERS FROM "file:///samples-cna-amp-genes.csv" AS row
MATCH (sample:Sample { internalID: row.InternalSampleID})
MATCH (gene:Gene {entrezGeneID: row.EntrezGeneID})
MERGE (sample)-[ma:CNA_AMP]->(gene)
ON CREATE SET ma.molecularProfileID = row.GeneticProfileID;

// warm the cache
// MATCH (n)
// OPTIONAL MATCH (n)-[r]->()
// RETURN count(n.prop) + count(r.prop);
// or use apoc
// CALL apoc.warmup.run()
