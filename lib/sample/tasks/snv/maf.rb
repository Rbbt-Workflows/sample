
def Rbbt.with_workflow(workflow, &block)
  begin
    Workflow.require_workflow workflow
  rescue
    Log.warn "No running workflow #{workflow} code due to exception: ", $!.message
    return
  end
  block.call
end

module Sample

  def self.mutation_classification(mis, type)
    if mis.any?
      case
      when mis.select{|mi| mi =~ /Frame/}.any?
        if type == "INS"
          return "Frame_Shift_Ins"
        else
          return "Frame_Shift_Del"
        end
      when mis.select{|mi| mi =~ /ENS.*P.*:([A-Z])\d+([A-Z])/ && $1 != $2 }.any?
        return "Missense_Mutation"
      when mis.select{|mi| mi =~ /ENS.*P.*:[A-Z]\d+\*/}.any?
        return "Nonsense_Mutation"
      when mis.select{|mi| mi =~ /ENS.*P.*:\*\d+[A-Z]/ }.any?
        return "Nonstop_Mutation"
      when mis.select{|mi| mi =~ /ENS.*P.*:Indel/}.any?
        if type == "INS"
          return "In_Frame_Ins"
        else
          return "In_Frame_Del"
        end
      when mis.select{|mi| mi =~ /ENS.*T.*:UTR3/ }.any?
        return "3'UTR"
      when mis.select{|mi| mi =~ /ENS.*T.*:UTR5/ }.any?
        return "5'UTR"
      when mis.select{|mi| mi =~ /ENS.*P.*:([A-Z\*])\d+([A-Z\*])/ && $1 == $2 }.any?
        return "Silent"
      else
        raise "Unkown: #{mis * ", "}"
      end
    else
      classification = "IGR"
    end
  end

  MAF_FIELDS = %w(
    Hugo_Symbol
    Ensembl_Gene_Id
    Center
    NCBI_Build
    Chromosome
    Start_Position
    End_Position
    Strand
    Variant_Classification
    Variant_Type
    Reference_Allele
    Tumor_Seq_Allele1
    Tumor_Seq_Allele2
    dbSNP_RS
    dbSNP_Val_Status
    Tumor_Sample_Barcode
    Matched_Norm_Sample_Barc
    Match_Norm_Seq_Allele1
    Match_Norm_Seq_Allele2
    Tumor_Validation_Allele1
    Tumor_Validation_Allele2
    Match_Norm_Validation_Allele1
    Match_Norm_Validation_Allele2
    Verification_Status
    Validation_Status
    Mutation_Status
    Sequencing_Phase
    Sequence_Source
    Validation_Method
    Score
    BAM_File
    Sequencer
    Tumor_Sample_UUID
    Matched_Norm_Sample_UUID
  )


  MISSING = "MISSING"
  dep :genomic_mutations, :compute => :produce
  dep :genomic_mutation_consequence, :compute => :produce
  dep :sequence_ontology, :compute => :produce, :organism => :organism
  dep :organism
  dep Sequence, :reference, :positions => :genomic_mutations, :organism => :organism, :compute => :produce
  #dep do |jobname,options,dependencies|
  #  dependencies.collect{|d| d.rec_dependencies}.flatten.select{|dep| dep.task_name.to_s == 'expanded_vcf'}.first
  #end
  extension :maf
  task :maf_file => :tsv do

    organism = step(:organism).load

    ensg2name = Organism.identifiers(organism).index :target => "Associated Gene Name", :fields => ["Ensembl Gene ID"], :persist => true, :unnamed => true
    ensp2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Protein ID"], :persist => true, :unnamed => true
    enst2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Transcript ID"], :persist => true, :unnamed => true
    gene_strand = Organism.gene_positions(organism).tsv :fields => ["Strand"], :type => :single, :persist => true, :unnamed => true

    pasted = TSV.paste_streams([step(:reference), step(:sequence_ontology)])

    dumper = TSV::Dumper.new :key_field => "Mutation ID", :fields => MAF_FIELDS, :type => :list
    dumper.init(:header_hash => "", :preamble => false)
    TSV.traverse pasted, :type => :double, :into => dumper do |mutation,values|
      reference, gene, mis, mut_term, mi_term, so_term = values
      samples = [clean_name]

      mutation = mutation.first if Array === mutation
      reference = reference.first if Array === reference

      mutation_parts = mutation.split(":")

      result = []
      result.extend MultipleResult

      protein = mis.any? ? mis.first.split(":").first : nil
      if protein.nil?
        ensembl = gene.first
      else
        ensembl = ensp2ensg[protein] || enst2ensg[protein]
      end
      gene = ensg2name[ensembl] || "Unknown"


      chr = mutation_parts[0]
      start = mutation_parts[1]
      allele = mutation_parts[2]

      type = allele.length == 1 ? "SNP" : (allele[0] == '+' ? "INS" : "DEL")

      #classification = Study.mutation_classification(mis, type)
      classification = so_term.first if so_term

      center = ""
      build = ""

      eend = start.to_i + allele.length - 1
      strand = ensembl.nil? ? nil : gene_strand[ensembl] == 1 ? '+' : '-'


      reference = reference
      allele2 = allele
      rs = ""
      rs_validation = ""
      norm_barcode = ""
      norm_allele = ""
      norm_allele2 = ""
      validation_allele = ""
      validation_allele2 = ""
      norm_validation_allele = ""
      norm_validation_allele2 = ""
      verification = ""
      validation = ""
      status = ""
      phase = ""
      source = ""
      validation_method = ""
      score = ""
      bam = ""
      sequencer = ""
      norm_uuid = ""

      samples.each do |sample|
        barcode = sample
        uuid = sample

        values = []
        values << ensembl
        values << center
        values << build
        values << chr
        values << start
        values << eend
        values << strand
        values << classification
        values << type
        values << reference
        values << allele
        values << allele2
        values << rs
        values << rs_validation
        values << barcode
        values << norm_barcode
        values << norm_allele
        values << norm_allele2
        values << validation_allele
        values << validation_allele2
        values << norm_validation_allele
        values << norm_validation_allele2
        values << verification
        values << validation
        values << status
        values << phase
        values << source
        values << validation_method
        values << score
        values << bam
        values << sequencer
        values << uuid
        values << norm_uuid

        result << [mutation, [gene] + values]
      end
      result
    end
  end

  dep :genomic_mutations
  dep :maf_file
  dep :expanded_vcf, :canfail => true
  dep :organism
  dep Sequence, :transcript_offsets, :positions => :genomic_mutations, :organism => :organism
  extension :maf
  task :maf_file2 => :tsv do
    tsv = step(:maf_file).join.path.tsv :header_hash => ''
    tsv.key_field = "Genomic Mutation"
    mis = step(:mutated_isoforms_fast).load
    organism = step(:organism).load

    tsv = tsv.attach mis

    expanded_vcf = begin
                     step(:expanded_vcf)
                   rescue
                     nil
                   end

    if expanded_vcf
      tsv = tsv.attach step(:expanded_vcf).load
    end

    offsets = step(:transcript_offsets).load

    tsv.add_field "Standard gDNA mutation" do |mutation,values|
      chr, pos, alt = mutation.split(":")
      chr = "chr" + chr unless chr =~ /^chr/
      [chr + ":g." + pos + values["Reference_Allele"].first + ">" + values["Tumor_Seq_Allele1"].first]
    end

    transcript_5utr = Organism.transcript_5utr(organism).tsv(:single, :persist => true, :unnamed => true)

    tsv.add_field "Standard cDNA mutation" do |mutation,values|
      if offsets[mutation]
        offsets[mutation].collect do |v|
          transcript, position, strand = v.split(":")
          utr = transcript_5utr[transcript]
          cds_position = position.to_i - utr.to_i
          transcript + ":" + "c." + (cds_position.to_i + 1).to_s + values["Reference_Allele"].first + ">" + values["Tumor_Seq_Allele1"].first
        end 
      end
    end

    tsv.add_field "Standard protein mutation" do |mutation,values|
      mi = values["Mutated Isoform"].first
      next if mi.nil?
      protein, change = mi.split(":")
      next if change =~ /^UTR./
      ref, pos, alt = change.partition(/\d+/)
      ref, alt = [ref, alt].collect do |l| 
        Misc::THREE_TO_ONE_AA_CODE.keys[Misc::THREE_TO_ONE_AA_CODE.values.index(l)]
      rescue
        l
      end.collect{|code| [code[0].upcase, code[1..-1]] * ""}
      protein + ":" + "p." << ref << pos << alt
    end

    tsv
  end

  dep :genomic_mutations
  dep :organism
  dep Sequence, :reference, :positions => :genomic_mutations, :organism => :organism
  dep :genomic_mutation_annotations
  dep :sequence_ontology
  dep :DbNSFP, :principal => true
  dep :DbNSFP_pred, :principal => true
  dep :neo_epitopes, :principal => true
  dep Sequence, :genes, :positions => :genomic_mutations, :organism => :organism
  dep :expanded_vcf
  task :rtoledo_maf => :tsv do
    fields =<<-EOF.split("\n")
dbSNP
Ref
Alt
Type
Filter
Gene
Transcript
Exon
DNA_change
protein_change
CGI_input
SIFT_score
SIFT_pred
Polyphen2_HDIV_score
Polyphen2_HDIV_pred
Polyphen2_HVAR_score
Polyphen2_HVAR_pred
FATHMM_score
FATHMM_pred
gnomAD_exomes_AF
gnomAD_exomes_AC
gnomAD_exomes_AN
clinvar_clnsig
clinvar_trait
Cancer_Gene_Census
Biocarta_Pathway
KEGG_Pathway
MAF_tDNA
MAF_cfDNA
MAF_primary
    EOF

    organism = self.recursive_inputs[:organism]
    mutations = step(:genomic_mutations).load
    tsv = TSV.setup(mutations, :key_field => "Genomic Mutation", :fields => [], :type => :double, :namespace => organism)
    tsv.identifiers = Organism.identifiers(organism)

    annotations = step(:genomic_mutation_annotations).load
    consequence = step(:genomic_mutation_consequence).load
    reference = step(:reference).load
    reference.key_field = tsv.key_field

    tsv = tsv.attach annotations, :fields => ["DbSNP:RS ID", "GnomAD:AF"]
    tsv = tsv.attach reference, :fields => ["Reference Allele"]
    tsv.add_field "Alternative Allele" do |mutation|
      mutation.split(":")[2]
    end

    tsv = tsv.attach step(:sequence_ontology), :fields => ["SO Term"]

    genes = step(:genes).load.swap_id("Ensembl Gene ID", "Associated Gene Name")
    coding = Organism.gene_biotype(organism).tsv.change_key("Associated Gene Name").select("Biotype" => "protein_coding").keys
    genes.key_field = tsv.key_field

    tsv = tsv.attach genes, :fields => ["Associated Gene Name"]
    tsv.process "Associated Gene Name" do |genes|
      next if genes.nil?
      good = genes & coding
      good.any? ? good : genes
    end

    tsv.add_field "Mutated Isoform" do |mutation|
      mis = consequence[mutation] || []
    end

    tsv.add_field "Ensembl Protein ID" do |mutation|
      mis = consequence[mutation] || []
      mis.collect{|mi| mi.split(":").first}
    end

    tsv.add_field "AA Change" do |mutation|
      mis = consequence[mutation] || []
      mis.collect{|mi| mi.split(":").last}
    end

    tsv.attach step(:DbNSFP_pred).load, :fields => %w(SIFT_pred Polyphen2_HDIV_pred Polyphen2_HVAR_pred FATHMM_pred MetaSVM_pred)

    neo_epi = step(:neo_epitopes).load
    neo_epi_fixed = neo_epi.annotate({})
    neo_epi.each do |m,v|
      neo_epi_fixed[m.sub(/^chr/,'')] = v
    end
    tsv.attach neo_epi_fixed, :fields => ["MHCflurry MT Score", "MHCflurry WT Score"]


    tsv = Rbbt.with_workflow "Enrichment" do
      %w(kegg go_bp go_mf).each do |database| 
        enrichment = Enrichment.job(:enrichment, nil, :database => database, :list => genes.values.flatten, :organism => organism, :cutoff => 1.1, :fdr => false, :min_support => 0).run
        Log.tsv enrichment
        db_field = enrichment.key_field
        tsv.attach enrichment, :fields => [db_field]
        if tsv.fields.include? "GO ID"
          tsv.process "GO ID" do |go|
            GO.id2name go
          end 
          tsv.fields = tsv.fields.collect{|f| f == "GO ID" ? "GO ID (#{ database })" : f}
        end

      end
      tsv.swap_id "KEGG Pathway ID", "Pathway Name", :identifiers => KEGG.pathways
    end

    subset = case 
             when self.clean_name.include?('p136')
               "tumor"
             when self.clean_name.include?('IMN')
               "cfDNA"
             else
               "AltBaj"
             end

    exp_vcf = step(:expanded_vcf).load
    exp_vcf.fields = exp_vcf.fields.collect{|f| f.include?("normal:") ? f.sub("normal:", "germline_#{subset}:") : f }
    freq_fields = exp_vcf.fields.select{|f| %w(AD AF DP).include? f.split(":").last}
    tsv.attach exp_vcf, :fields => freq_fields


    tsv
  end
end
