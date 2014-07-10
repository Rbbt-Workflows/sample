require 'rbbt/entity'

module Sample
  extend Entity
  
  Sample.tasks.each do |name, b|
    property name.to_sym => :single do |run=true|
      job = Sample.sample_job Sample, name.to_sym, self, {}
      case run
      when nil, TrueClass
        job.run 
      when :path
        job.run(true).join.path
      when :job
        job
      end
    end
  end
end
