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

  helper :watson do
    Sample.watson sample
  end

  input :organism, :string, "Organism code", nil
  task :organism => :string do |_organism|
    _organism || organism
  end

  input :watson, :boolean, "Mutations given in the watson strand", nil
  task :watson => :string do |_watson|
    _watson.nil? ? watson : _watson
  end

  def self._sample_dep(workflow, task)
    dep workflow, task do |sample, options|
      Sample.sample_job(workflow, task, sample, options)
    end
  end
end

require 'sample'
require 'sample/tasks/genomic_mutations'
require 'sample/tasks/mutated_isoforms'
require 'sample/tasks/genes'
require 'sample/tasks/vcf'
require 'sample/tasks/cnvs'
require 'rbbt/entity/sample'

Workflow.require_workflow "MutationSignatures"
require 'sample/tasks/mutation_signatures'
