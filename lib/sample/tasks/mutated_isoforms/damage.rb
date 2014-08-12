Workflow.require_workflow "DbNSFP"

module Sample
  dep :ns_mutated_isoforms
  dep DbNSFP, :annotate, :mutations => :ns_mutated_isoforms
  task :db_NSFP => :tsv do
    TSV.get_stream step(:annotate)
  end

  dep :db_NSFP
  input :damage_field, :string, "DbNSFP column value to threshold", "RadialSVM_score"
  input :damage_threshold, :float, "Min. value threshold", 0
  task :damaged_mi => :array do |field,threshold|
    TSV.traverse step(:db_NSFP), :into => :stream, :fields => [field], :type => :single, :cast => :to_f do |mutation, value|
      next unless value and value >= threshold
      Array === mutation ? mutation.first : mutation
    end
  end

  dep :ns_mutated_isoforms
  task :truncated_mi => :array do 
    TSV.traverse step(:ns_mutated_isoforms), :type => :array, :into => :stream do |mi|
      next unless mi =~ /:.*\d+(FrameShift|\*)$/
      mi
    end
  end

  dep :damaged_mi
  dep :truncated_mi
  dep :consequence
  task :damaging => :tsv do
    damaged_mi = Set.new(step(:damaged_mi).load)
    truncated_mi = Set.new(step(:truncated_mi).load)

    broken_mis = damaged_mi + truncated_mi

    TSV.traverse step(:consequence), :into => :dumper, :type => :flat,
      :key_field => "Genomic Mutation",
      :fields => ["Mutated Isoform"] do |mutation, mis|
      broken = broken_mis & mis
      next if broken.empty?
      mutation = Array === mutation ? mutation.first : mutation

      [mutation, broken.to_a]
    end
  end
end
