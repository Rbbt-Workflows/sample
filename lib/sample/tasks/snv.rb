Workflow.require_workflow "Sequence"

require 'sample/tasks/snv/vcf'
require 'sample/tasks/snv/genomic_mutations'
require 'sample/tasks/snv/common'
require 'sample/tasks/snv/maf'

Sample.instance_eval &SNVTasks

require 'sample/tasks/snv/zygosity'
require 'sample/tasks/snv/genes'
#require 'sample/tasks/snv/drugs'

#require 'sample/tasks/snv/mutated_isoforms'
#require 'sample/tasks/snv/damage'
#require 'sample/tasks/snv/structureppi'

require 'rbbt/entity/sample'
Sample.update_task_properties
