require 'rbbt-util'
require 'rbbt/workflow'

module Sample
  extend Workflow

  helper :sample do
    clean_name
  end

  helper :organism do
    Sample.organism sample
  end

  def self.sample_dep(workflow, task)
    dep workflow, task do |sample, options|
      Sample.sample_job(workflow, task, sample, options)
    end
  end
end

require 'sample/sample'
require 'sample/tasks/genomic_mutations'
require 'sample/tasks/mutated_isoforms'
require 'sample/tasks/genes'
require 'sample/tasks/vcf'
require 'sample/sample/entity'
