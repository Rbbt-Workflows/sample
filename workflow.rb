require 'rbbt-util'
require 'rbbt/workflow'

Workflow.require_workflow "Sequence"
Workflow.require_workflow "Structure"

module Sample
  extend Workflow

  helper :sample do
    clean_name
  end

  helper :organism do
    Sample.organism sample
  end

  export_asynchronous :mutated_isoforms, :annotations, :neighbour_annotations, :annotate_vcf
end

require 'sample/annotate_vcf'
require 'sample/sample'
require 'sample/tasks/vcf'
require 'sample/tasks/genomic_mutations'
require 'sample/tasks/mutated_isoforms'
require 'sample/tasks/annotations'
