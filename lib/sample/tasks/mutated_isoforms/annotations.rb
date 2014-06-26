Workflow.require_workflow "Structure"
Workflow.require_workflow "DbNSFP"

module Sample
  dep :ns_mutated_isoforms
  dep DbNSFP, :annotate do |sample, options, dependencies|
    DbNSFP.job(:annotate, sample, :mutations => dependencies.first)
  end
  task :db_NSFP => :tsv do
    TSV.get_stream step(:annotate)
  end

  dep :ns_mutated_isoforms
  dep Structure, :mi_interfaces do |sample, options, dependencies|
    Sample.sample_job(Structure, :mi_interfaces, sample, :mutated_isoforms => dependencies.first)
  end
  task :interfaces => :tsv do
    TSV.get_stream step(:mi_interfaces)
  end

  dep :ns_mutated_isoforms
  Structure::ANNOTATORS.keys.sort.collect do |database|
    next if database == "COSMIC"
    dep Structure, :annotate_mi do |sample, options, dependencies|
      Sample.sample_job(Structure, :annotate_mi, sample, :database => database, :mutated_isoforms => dependencies.first)
    end
  end
  task :structure_annotations => :tsv do
    TSV.paste_streams dependencies[1..-1], :sort => true
  end

  dep :ns_mutated_isoforms
  Structure::ANNOTATORS.keys.sort.collect do |database|
    next if database == "COSMIC"
    dep Structure, :annotate_mi_neighbours do |sample, options, dependencies|
      Sample.sample_job(Structure, :annotate_mi_neighbours, sample, :database => database, :mutated_isoforms => dependencies.first)
    end
  end
  task :structure_neighbour_annotations => :tsv do
    TSV.paste_streams dependencies[1..-1], :sort => true
  end

  dep :structure_neighbour_annotations
  task :neighbours => :tsv do
    TSV.get_stream step(:structure_neighbour_annotations).step(:annotate_mi_neighbours).step(:mi_neighbours)
  end
end
