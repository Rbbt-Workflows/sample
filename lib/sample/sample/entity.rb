require 'rbbt/entity'

module Sample
  extend Entity

  self.annotation :cohort

  property :sample_code => :single do
    if cohort.nil? or cohort.empty? or self =~ /^#{ cohort }:/
      sample_code
    else
      cohort + ':' << self
    end
  end
  
  Sample.tasks.each do |name, b|
    property name.to_sym => :single do |run=true|
      job = Sample.sample_job Sample, name.to_sym, sample_code, {}
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

  property :has_genotype? => :single do
    ! Sample.sample_dir(sample_code).nil?
  end
end
