Workflow.require_workflow "Proteomics"

module Sample

  dep :ns_mutated_isoforms
  dep Proteomics, :mi_interfaces, :mutated_isoforms => :ns_mutated_isoforms, :organism => :organism
  task :interfaces => :tsv do
    TSV.get_stream step(:mi_interfaces)
  end

  dep :ns_mutated_isoforms
  Proteomics::ANNOTATORS.keys.sort.collect do |database|
    next if database == "COSMIC"
    dep Proteomics, :annotate_mi, :database => database.to_s, :mutated_isoforms => :ns_mutated_isoforms, :organism => :organism
  end
  task :structure_annotations => :tsv do
    TSV.paste_streams dependencies[1..-1], :sort => true
  end

  dep :ns_mutated_isoforms
  Proteomics::ANNOTATORS.keys.sort.collect do |database|
    next if database == "COSMIC"
    dep Proteomics, :annotate_mi_neighbours, :database => database.to_s, :mutated_isoforms => :ns_mutated_isoforms, :organism => :organism
  end
  task :structure_neighbour_annotations => :tsv do
    TSV.paste_streams dependencies[1..-1], :sort => true
  end

  dep :structure_neighbour_annotations
  task :neighbours => :tsv do
    TSV.get_stream step(:structure_neighbour_annotations).step(:annotate_mi_neighbours).step(:mi_neighbours)
  end
end
