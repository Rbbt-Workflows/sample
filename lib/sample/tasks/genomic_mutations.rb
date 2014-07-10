Workflow.require_workflow "DbSNP"
Workflow.require_workflow "Sequence"
Workflow.require_workflow "EVS"
Workflow.require_workflow "Genomes1000"
Workflow.require_workflow "GERP"

module Sample

  sample_dep GERP, :annotate 
  sample_dep EVS, :annotate 
  task :genomic_mutation_annotations => :tsv do 
    TSV.paste_streams(dependencies, :sort => false)
  end

  sample_dep Sequence, :affected_genes
  task :compound_mutations => :array do
    tsv = step(:affected_genes).join.path.tsv :key_field => "Ensembl Gene ID", :merge => true, :type => :flat
    tsv.select{|g,ms| ms.length > 1 }.values.flatten.compact.sort.uniq
  end
end
