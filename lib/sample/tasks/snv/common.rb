Workflow.require_workflow "Sequence"
Workflow.require_workflow "InterPro"
Workflow.require_workflow "Structure"
Workflow.require_workflow "GERP"
Workflow.require_workflow "DbSNP"
Workflow.require_workflow "DbNSFP"
Workflow.require_workflow "EVS"


require 'rbbt/sources/InterPro'

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

  dep :genomic_mutations
  task :num_genomic_mutations => :integer do
    io = TSV.get_stream step(:genomic_mutations)
    CMD.cmd('wc -l', :in => io).read
  end

  dep :annotate_DbSNP
  dep :annotate_Genomes1000
  dep :annotate_GERP
  dep :annotate_EVS
  task :genomic_mutation_annotations => :tsv do
    TSV.paste_streams dependencies, :sort => true
  end

  dep :organism
  dep :genomic_mutations
  dep Sequence, :genes, :positions => :genomic_mutations, :organism => :organism, :vcf => false
  task :genomic_mutation_gene_overlaps => :tsv do
    TSV.get_stream step(:genes)
  end

  dep :organism
  dep :genomic_mutations
  dep Sequence, :splicing_mutations, :mutations => :genomic_mutations, :organism => :organism, :vcf => false, :watson => true
  task :genomic_mutation_splicing_consequence => :tsv do
    TSV.get_stream step(:splicing_mutations)
  end

  dep :organism
  dep :watson
  dep :genomic_mutations
  dep Sequence, :mutated_isoforms_fast, :mutations => :genomic_mutations, :organism => :organism, :vcf => false, :watson => :watson, :coding => true
  task :genomic_mutation_consequence => :tsv do
    TSV.get_stream step(:mutated_isoforms_fast)
  end

  dep :genomic_mutation_consequence, :non_synonymous => true
  task :mi => :array do
    TSV.traverse step(:genomic_mutation_consequence), :into => :stream, :bar => "Processing MIs" do |mut, mis|
      mis = mis.reject{|mi| mi =~ /ENST|:([*A-Z])\d+\1$/}
      next if mis.empty?
      mis.extend MultipleResult
      mis
    end
  end

  dep :mi
  task :mi_truncated => :array do 
    ensp2sequence = Organism.protein_sequence(organism).tsv :persist => true, :unnamed => true
    ensp2uni = Organism.identifiers(organism).index :target => "UniProt/SwissProt Accession", :persist => true, :fields => ["Ensembl Protein ID"], :unnamed => true
    domain_info = InterPro.protein_domains.tsv :persist => true, :unnamed => true
    TSV.traverse step(:mi), :type => :array, :into => :stream, :bar => "MI truncated" do |mi|
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
  dep DbNSFP, :score, :mutations => :mi, :organism => :organism
  task :DbNSFP => :tsv do
    TSV.get_stream step(:score)
  end

  dep :DbNSFP
  input :field, :string, "Damage score field from DbNSFP", "MetaSVM_score"
  task :mi_damaged => :array do |field|
    TSV.traverse step(:DbNSFP), :fields => [field], :type => :single, :cast => :to_f, :into => :stream, :bar => "MI damaged" do |mi, score|
      next nil unless score > 0
      mi.extend MultipleResult if Array === mi
      mi
    end
  end

  dep :mi #, :principal => true
  task :kinmut => :tsv do
    begin
      Workflow.require_workflow "KinMut2"
      job = KinMut2.job(:predict_fix, clean_name, :mutations => step(:mi))
      job.produce

      parser = TSV::Parser.new(job.path)
      options = parser.options
      options[:fields] = options[:fields] + ["Ensembl Protein ID"]
      dumper = TSV::Dumper.new options
      dumper.init 
      TSV.traverse job, :into => dumper do |mi,values|
        mi = mi.first if Array === mi
        values.push mi.partition(":").first
        [mi, values]
      end

      translations = job.step(:predict).file('translations').tsv :type => :list
      translations.key_field = "Ensembl Protein ID"
      TSV.open(dumper).attach(translations).reorder :key, parser.fields + translations.fields
    rescue Exception
      Log.warn "KinMut error: " << $!.message
      Log.exception $!
      ""
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

  dep :interfaces
  task :broken_ppi => :tsv do
    ensp2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Protein ID"], :persist => true
    dumper = TSV::Dumper.new :key_field => "Mutated Isoform", :fields => ["Ensembl Gene ID", "Partner (Ensembl Gene ID)"], :type => :list, :namespace => organism
    dumper.init
    TSV.traverse step(:interfaces), :into => dumper do |mi, values|
      mi = mi.first if Array === mi
      partners = values.first
      source = mi.partition(":").first
      ps = partners.collect do |partner|
        [mi,ensp2ensg.values_at(source, partner)]
      end
      ps.extend MultipleResult
      ps
    end
  end

  dep :mi
  dep :organism
  dep Structure, :annotate_mi, :mutated_isoforms => :mi, :organism => :organism
  task :annotate_mi => :tsv do
    TSV.get_stream(step(:annotate_mi))
  end

  dep :mi
  dep :organism
  dep Structure, :annotate_mi_neighbours, :mutated_isoforms => :mi, :organism => :organism
  task :annotate_mi_neighbours => :tsv do
    TSV.get_stream(step(:annotate_mi_neighbours))
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
  dep Sequence, :TSS, :positions => :genomic_mutations, :organism => :organism, :vcf => false
  task :TSS => :tsv do
    TSV.get_stream step(:TSS)
  end

  dep :DbNSFP
  task :gene_damage_bias => :tsv do 

    damage_field = "MetaLR_score"
    protein_bg_scores = TSV.setup({}, :key_field => "Ensembl Protein ID", :fields => ["Score"], :type => :flat)

    protein_scores = {}
    TSV.traverse step(:DbNSFP), :fields => [damage_field], :type => :single, :bar => self.progress_bar("Computing protein damage scores") do |mi, score|
      mi = mi.first if Array === mi
      next unless mi =~ /ENSP/
      next if score == -999
      protein = mi.split(":").first
      next unless Appris::PRINCIPAL_ISOFORMS.include? protein
      protein_scores[protein] ||= []
      protein_scores[protein] << score
      nil
    end

    bg_proteins = protein_scores.select{|k,v| v.length >= 3}.collect{|k,v| k}
    TSV.traverse bg_proteins, :fields => [damage_field], :type => :array, :cpus => 10, :bar => self.progress_bar("Computing background protein damage scores"), :into => protein_bg_scores do |protein|
      scores = begin
                all_protein_mis = DbNSFP.job(:possible_mutations, clean_name + ' ' + protein, :protein => protein).exec
                if all_protein_mis
                  prediction_job = DbNSFP.job(:score, "all_" + protein, :mutations => all_protein_mis)
                  prediction_job.produce
                  TSV.open(Open.open(prediction_job.path, :nocache => true), :fields => [damage_field], :type => :single, :cast => :to_f, :unnamed => true).values.flatten.compact.reject{|v| v == -999 }
                else
                  nil
                end
              rescue
                Log.exception $!
                nil
              end
      next if scores.nil?
      [protein, scores]
    end


    tsv = TSV.setup({}, :key_field => "Ensembl Protein ID", :fields => ["Score Avg.", "Background Score Avg.", "p.value"], :type => :list, :namespace => organism, :cast => :to_f)
    protein_scores.each do |protein,scores|
      next if scores.nil? or scores.length < 3
      bg_scores = protein_bg_scores[protein]
      next if bg_scores.nil? or bg_scores.length < 3
      pvalue = R.eval_a "t.test(#{R.ruby2R scores}, #{R.ruby2R bg_scores}, alternative='greater')$p.value"
      tsv[protein] = [Misc.mean(scores) || scores.first, Misc.mean(bg_scores) || bg_scores.first, pvalue]
    end

    fields = tsv.fields
    ensp2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :persist => true, :fields => ["Ensembl Protein ID"]
    tsv.add_field "Ensembl Gene ID" do |protein, scores|
      ensp2ensg[protein]
    end
    tsv.reorder "Ensembl Gene ID", fields
  end

  dep :organism
  dep :genomic_mutations
  dep Sequence, :sequence_ontology, :mutations => :genomic_mutations, :organism => :organism, :vcf => false
  task :sequence_ontology => :tsv do
    TSV.get_stream step(:sequence_ontology)
  end

  dep :genomic_mutations
  dep Sequence, :intersect_bed, :positions => :genomic_mutations
  task :intersect_bed => :tsv do
    TSV.get_stream step(:intersect_bed)
  end

end
