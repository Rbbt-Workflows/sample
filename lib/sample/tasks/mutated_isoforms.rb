Workflow.require_workflow "DbNSFP"
module Sample

  dep do |sample,options|
    Structure::ANNOTATORS.keys.sort.collect do |database|
      next if database == "COSMIC"
      Sample.sample_job(Structure, :annotate, sample, options.merge({:database => database}))
    end
  end
  input :principal, :boolean, "Use only principal isoforms", true
  task :annotations => :tsv do |principal|
    jobs = dependencies.each do |dep| dep.grace end

    clean_pos = nil
    TSV.traverse TSV.paste_streams(jobs), :into => :stream, :type => :array do |line|
      next line if line =~ /^#:/
      if line =~ /^#/
          clean_pos = []
          key, *fields = line.split("\t",-1)
          fields.each_with_index do |f,i|
            clean_pos << i unless f == "Mutated Isoform" or f == "Residue" or f == "Genomic Mutation"
          end
          key << "\t" << fields.values_at(*clean_pos) * "\t"
      else
        k, *rest = line.split("\t",-1)
        k << "\t" << rest.values_at(*clean_pos)*"\t"
      end
    end
  end

  dep do |sample,options|
    Structure::ANNOTATORS.keys.collect do |database|
      next if database == "COSMIC"
      Sample.sample_job(Structure, :annotate_neighbours, sample, options.merge({:database => database})) #.run(true).grace
    end
  end
  input :principal, :boolean, "Use only principal isoforms", true
  task :neighbour_annotations => :tsv do |principal|
    clean_pos = nil
    TSV.traverse TSV.paste_streams(jobs), :into => :stream, :type => :array do |line|
      next line if line =~ /^#:/
      if line =~ /^#/
          clean_pos = []
          key, *fields = line.split("\t",-1)
          fields.each_with_index do |f,i|
            clean_pos << i unless f == "Mutated Isoform" or f == "Residue" or f == "Genomic Mutation"
          end
          key << "\t" << fields.values_at(*clean_pos) * "\t"
      else
        k, *rest = line.split("\t",-1)
        k << "\t" << rest.values_at(*clean_pos)*"\t"
      end
    end
  end

  dep :ns_mutated_isoforms
  task :db_NSFP => :tsv do
    TSV.get_stream DbNSFP.job(:annotate, name, :mutations => step(:ns_mutated_isoforms)).run(true)
  end
end
