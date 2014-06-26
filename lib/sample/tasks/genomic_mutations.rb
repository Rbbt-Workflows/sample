Workflow.require_workflow "DbSNP"
Workflow.require_workflow "EVS"
Workflow.require_workflow "Genomes1000"
Workflow.require_workflow "GERP"

module Sample

  sample_dep GERP, :annotate 
  sample_dep EVS, :annotate 
  task :genomic_mutation_annotations => :tsv do 
    TSV.paste_streams(dependencies, :sort => false)
  end
end
