#Workflow.require_workflow "DbNSFP"
#
#module Sample
#  dep :ns_mutated_isoforms
#  task :truncated_mi => :array do 
#    TSV.traverse step(:ns_mutated_isoforms), :type => :array, :into => :stream do |mi|
#      next unless mi =~ /:.*\d+(FrameShift|\*)$/
#      mi
#    end
#  end
#
#  dep :ns_mutated_isoforms
#  dep DbNSFP, :annotate, :mutations => :ns_mutated_isoforms
#  task :db_NSFP => :tsv do
#    TSV.get_stream step(:annotate)
#  end
#
#  dep :db_NSFP
#  input :damage_field, :string, "DbNSFP column value to threshold", "MetaSVM_score"
#  input :damage_threshold, :float, "Min. value threshold", 0
#  task :damaged_predicted_mi => :array do |field,threshold|
#    TSV.traverse step(:db_NSFP), :into => :stream, :fields => [field], :type => :single, :cast => :to_f do |mutation, value|
#      next unless value and value >= threshold
#      Array === mutation ? mutation.first : mutation
#    end
#  end
#
#  dep :damaged_predicted_mi
#  dep :truncated_mi
#  task :damaged_mi => :array do 
#    streams = dependencies.collect{|dep| TSV.get_stream dep }
#    Misc.intercalate_streams streams
#  end
#end
