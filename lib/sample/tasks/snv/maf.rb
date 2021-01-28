
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
      reference, mis, mut_term, mi_term, so_term = values
      samples = [clean_name]

      mutation = mutation.first if Array === mutation
      reference = reference.first if Array === reference

      mutation_parts = mutation.split(":")

      result = []
      result.extend MultipleResult

      protein = mis.any? ? mis.first.split(":").first : nil
      ensembl = ensp2ensg[protein] || enst2ensg[protein]
      gene = ensg2name[ensembl] || "Unknown"

      chr = mutation_parts[0]
      start = mutation_parts[1]
      allele = mutation_parts[2]

      type = allele.length == 1 ? "SNP" : (allele[0] == '+' ? "INS" : "DEL")

      #classification = Study.mutation_classification(mis, type)
      classification = so_term.first

      center = ""
      build = ""

      eend = start.to_i + allele.length - 1
      strand = gene_strand[ensembl] == 1 ? '+' : '-'


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

    tsv.add_field "Standard cDNA mutation" do |mutation,values|
      if offsets[mutation]
        offsets[mutation].collect do |v|
          transcript, position, strand = v.split(":")
          transcript + ":" + "c." + (position.to_i + 1).to_s + values["Reference_Allele"].first + ">" + values["Tumor_Seq_Allele1"].first
        end 
      end
    end

    tsv
  end

end
