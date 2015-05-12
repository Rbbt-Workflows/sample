require 'sample/tasks/mutated_isoforms/consequence'
require 'sample/tasks/mutated_isoforms/annotations'
require 'sample/tasks/mutated_isoforms/damage'

module Sample

  dep :structure_annotations 
  dep :structure_neighbour_annotations 
  dep :interfaces 
  dep :db_NSFP 
  task :mutated_isoform_annotations => :tsv do 
    TSV.paste_streams(dependencies, :sort => true)
  end

  dep :consequence
  dep :mutated_isoform_annotations
  task :mutation_mi_annotations => :tsv do 
    Step.wait_for_jobs dependencies
    annotations = TSV.open(step(:mutated_isoform_annotations), :unnamed => true)

    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => ["Mutated Isoform"] + annotations.fields, :type => :double, :namespace => organism
    dumper.init

    TSV.traverse step(:consequence), :bar => "Mutation MI annotations", :into => dumper do |mutation, isoforms|
      mutation = mutation.first if Array === mutation
      values = []
      isoforms.each do |iso|
        iso_values = annotations[iso]
        next if iso_values.nil? or iso_values.flatten.compact.empty?
        values << [iso] + iso_values.collect{|v| v * ";;"}
      end
      next if values.empty?
      [mutation, Misc.zip_fields(values)]
    end

    Misc.sort_stream dumper.stream
  end
end
