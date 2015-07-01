Workflow.require_workflow "Sequence"
Workflow.require_workflow "Structure"
Workflow.require_workflow "GERP"
Workflow.require_workflow "DbSNP"
Workflow.require_workflow "DbNSFP"
Workflow.require_workflow "EVS"

SNVTasks = Proc.new do

  dep :genomic_mutations
  dep :organism 
  dep GERP, :annotate, :mutations => :genomic_mutations, :organism => :organism
  task :annotate_GERP => :tsv do
    TSV.get_stream step(:annotate)
  end
  
  dep :genomic_mutations
  dep :organism 
  dep DbSNP, :annotate, :mutations => :genomic_mutations, :organism => :organism
  task :annotate_DbSNP => :tsv do
    TSV.get_stream step(:annotate)
  end

  dep :genomic_mutations
  dep :organism 
  dep Genomes1000, :annotate, :mutations => :genomic_mutations, :organism => :organism
  task :annotate_Genomes1000 => :tsv do
    TSV.get_stream step(:annotate)
  end

  dep :genomic_mutations
  dep :organism 
  dep EVS, :annotate, :mutations => :genomic_mutations, :organism => :organism
  task :annotate_EVS => :tsv do
    TSV.get_stream step(:annotate)
  end

  dep :annotate_DbSNP
  dep :annotate_Genomes1000
  dep :annotate_GERP
  dep :annotate_EVS
  task :genomic_mutation_annotations => :tsv do
    TSV.paste_streams dependencies, :sort => true
  end

  dep :genomic_mutations
  dep Sequence, :genes, :positions => :genomic_mutations, :organism => :organism
  task :genomic_mutation_gene_overlaps => :tsv do
    TSV.get_stream step(:genes)
  end

  dep :genomic_mutations
  dep Sequence, :splicing_mutations, :mutations => :genomic_mutations, :organism => :organism
  task :genomic_mutation_splicing_consequence => :tsv do
    TSV.get_stream step(:splicing_mutations)
  end

  dep :genomic_mutations
  dep Sequence, :mutated_isoforms_fast, :mutations => :genomic_mutations, :organism => :organism
  task :genomic_mutation_consequence => :tsv do
    TSV.get_stream step(:mutated_isoforms_fast)
  end

  dep :genomic_mutation_consequence
  task :mi => :array do
    io = TSV.traverse step(:genomic_mutation_consequence), :into => :stream do |mut, mis|
      mis = mis.reject{|mi| mi =~ /ENST|:([*A-Z])\d+\1$/}
      next if mis.empty?
      mis.extend MultipleResult
      mis
    end
    CMD.cmd('shuf', :in => io, :pipe => true)
  end

  dep :mi
  task :mi_truncated => :array do 
    ensp2sequence = Organism.protein_sequence(organism).tsv :persist => true, :unnamed => true
    ensp2uni = Organism.identifiers(organism).index :target => "UniProt/SwissProt Accession", :persist => true, :fields => ["Ensembl Protein ID"], :unnamed => true
    domain_info = InterPro.protein_domains.tsv :persist => true, :unnamed => true
    TSV.traverse step(:mi), :type => :array, :into => :stream do |mi|
      next unless mi =~ /:.*(\d+)(FrameShift|\*)$/
      pos = $1.to_i
      protein = mi.partition(":")[0]
      sequence = ensp2sequence[protein]
      next unless sequence
      uni = ensp2uni[protein]
      ablated_domains = []
      if uni
        domains = domain_info[uni]
        if domains
          Misc.zip_fields(domains).each do |domain,start,eend|
            if eend.to_i > pos
              ablated_domains << domain
            end
          end
        end
      end
      next unless pos < (sequence.length.to_f * 0.7) or ablated_domains.any?
      mi
    end
  end

  dep :mi
  dep :organism
  dep DbNSFP, :annotate, :mutations => :mi, :organism => :organism
  task :DbNSFP => :tsv do
    TSV.get_stream step(:annotate)
  end

  Workflow.require_workflow "KinMut2"
  dep :mi
  task :kinmut => :tsv do
    begin
      KinMut2.job(:predict_fix, clean_name, :mutations => step(:mi)).run
    rescue Exception
      Log.warn "KinMut error: " << $!.message
      ""
    end
  end

  dep :DbNSFP
  input :field, :string, "Damage score field from DbNSFP", "MetaSVM_score"
  task :mi_damaged => :array do |field|
    TSV.traverse step(:DbNSFP), :fields => [field], :type => :single, :cast => :to_f, :into => :stream do |mi, score|
      next nil unless score > 0
      mi.extend MultipleResult if Array === mi
      mi
    end
  end

  dep :mi
  dep :organism
  dep Structure, :mi_interfaces, :mutated_isoforms => :mi, :organism => :organism
  task :interfaces => :tsv do
    parser = TSV::Parser.new step(:mi_interfaces)
    dumper = TSV::Dumper.new parser.options.merge(:fields => ["Partner Ensembl Protein ID"])
    dumper.init
    TSV.traverse parser, :into => dumper do |mi, values|
      mi = mi.first if Array === mi
      [mi, [values[1].uniq]]
    end
  end

  dep :mi
  dep :organism
  dep Structure, :annotate_mi, :mutated_isoforms => :mi, :organism => :organism, :database => "Appris"
  task :firestar => :tsv do
    fields = ["Appris Feature", "Appris Feature Description", "Appris Feature Range"]
    parser = TSV::Parser.new step(:annotate_mi)
    dumper = TSV::Dumper.new parser.options
    dumper = TSV::Dumper.new parser.options.merge(:fields => ["Firestar site", "Firestar range"])
    dumper.init
    TSV.traverse parser, :fields => fields, :into => dumper do |mi, values|
      next unless values[0].include? "firestar"
      mi = mi.first if Array === mi

      filtered = []
      Misc.zip_fields(values).each do |name,range,desc|
        next unless name == 'firestar'
        filtered << [desc, range]
      end

      next if filtered.empty?
      [mi, Misc.zip_fields(filtered)]
    end
  end

  dep :mi
  dep :organism
  dep Structure, :annotate_mi_neighbours, :mutated_isoforms => :mi, :organism => :organism, :database => "Appris"
  task :firestar_neighbours => :tsv do
    fields = ["Appris Feature", "Appris Feature Description", "Appris Feature Range"]
    parser = TSV::Parser.new step(:annotate_mi_neighbours)
    dumper = TSV::Dumper.new parser.options.merge(:fields => ["Firestar neighbour site", "Firestar neighbour range"])
    dumper.init
    TSV.traverse parser, :fields => fields, :into => dumper do |mi, values|
      next unless values[1].include? "firestar"
      mi = mi.first if Array === mi

      filtered = []
      Misc.zip_fields(values).each do |res,name,range,desc|
        next unless name == 'firestar'
        filtered << [desc, range]
      end

      next if filtered.empty?
      [mi, Misc.zip_fields(filtered)]
    end
  end

  dep :interfaces
  dep :firestar
  dep :firestar_neighbours
  task :mi_annotations => :tsv do
    TSV.paste_streams dependencies, :sort => true
  end

  dep :genomic_mutations
  dep :organism
  dep Sequence, :TSS, :positions => :genomic_mutations, :organism => :organism
  task :TSS => :tsv do
    TSV.get_stream step(:TSS)
  end

  dep :DbNSFP
  task :gene_damage_bias => :tsv do 

    damage_field = "MetaLR_score"
    protein_bg_scores = {}
    protein_scores = {}
    TSV.traverse step(:DbNSFP), :fields => [damage_field], :type => :single, :bar => self.progress_bar("Traversing protein mutation scores") do |mi, score|
      mi = mi.first if Array === mi
      next unless mi =~ /ENSP/
      next if score == -999
      protein = mi.split(":").first
      protein_bg_scores[protein] ||= begin
                                       all_protein_mis = DbNSFP.job(:possible_mutations, clean_name + ' ' + protein, :protein => protein).exec
                                       if all_protein_mis
                                         prediction_job = DbNSFP.job(:annotate, "all_" + protein, :mutations => all_protein_mis)
                                         prediction_job.produce
                                         prediction_job.path.tsv(:fields => [damage_field], :type => :single, :cast => :to_f).values.flatten.compact.reject{|v| v == -999 }
                                       else
                                         nil
                                       end
                                     rescue
                                       Log.exception $!
                                       nil
                                     end
      protein_scores[protein] ||= []
      protein_scores[protein] << score
      nil
    end

    tsv = TSV.setup({}, :key_field => "Ensembl Protein ID", :fields => ["Score Avg.", "Background Score Avg.", "p.value"], :type => :list, :namespace => organism)
    protein_scores.each do |protein,scores|
      next if scores.nil? or scores.length < 3
      bg_scores = protein_bg_scores[protein]
      next if bg_scores.nil? or bg_scores.length < 3
      pvalue = R.eval_a "t.test(#{R.ruby2R scores}, #{R.ruby2R bg_scores}, alternative='greater')$p.value"
      tsv[protein] = [Misc.mean(scores) || scores.first, Misc.mean(bg_scores) || bg_scores.first, pvalue]
    end
    tsv
  end
end
