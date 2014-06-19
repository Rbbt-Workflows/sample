require 'rbbt-util'
require 'rbbt/workflow'

Workflow.require_workflow "Sequence"
Workflow.require_workflow "Structure"

module Sample
  extend Workflow


  dep :affected_genes
  #dep :annotations
  #dep :neighbour_annotations
  #dep :interfaces
  dep :mutated_isoforms
  dep :ns_mutated_isoforms
  task :all => :string do
    Step.wait_for_jobs dependencies
    "DONE"
  end

  export_asynchronous :mutated_isoforms, :annotations, :neighbour_annotations, :annotate_vcf
end

require 'sample/annotate_vcf'
require 'sample/sample'
require 'sample/tasks/vcf'
require 'sample/tasks/genomic_mutations'
require 'sample/tasks/mutated_isoforms'
require 'sample/tasks/annotations'
