module Sample

  dep :affected_splicing
  dep :mutated_isoform
  dep :homozygous
  task :affected_alleles => :tsv do
    genes = {}
    homozygous = Set.new step(:homozygous).load
    TSV.traverse step(:affected_splicing) do |gene, mutations|
      mutations.collect do |mutation|
        genes[gene] ||= []
        if homozygous.include? mutation
          genes[gene] << "H:SPLICING"
        else
          genes[gene] << "SPLICING" 
        end
      end
    end

    TSV.traverse step(:mutated_isoform) do |gene, mutations|
      mutations.collect do |mutation|
        genes[gene] ||= []
        if homozygous.include? mutation
          genes[gene] << "H:MUTATED_ISOFORM"
        else
          genes[gene] << "MUTATED_ISOFORM"
        end
      end
    end

    res = TSV.setup({}, :key_field => "Ensembl Gene ID", :fields => ["Affected alleles"], :namespace => organism, :type => :single)
    genes.each do |gene,alts|
      broken = if alts.select{|a| a =~ /^H:/ }.any?
                 "BOTH"
               elsif alts.length > 1
                 "BOTH?"
               else
                 "ONE"
               end
      res[gene] = broken
    end
    res
  end

  dep :affected_splicing
  dep :broken_isoform
  dep :homozygous
  task :broken_alleles => :tsv do
    genes = {}
    homozygous = Set.new step(:homozygous).load

    TSV.traverse step(:affected_splicing) do |gene, mutations|
      mutations.collect do |mutation|
        genes[gene] ||= []
        if homozygous.include? mutation
          genes[gene] << "H:SPLICING"
        else
          genes[gene] << "SPLICING" 
        end
      end
    end

    TSV.traverse step(:broken_isoform) do |gene, mutations|
      mutations.collect do |mutation|
        genes[gene] ||= []
        if homozygous.include? mutation
          genes[gene] << "H:MUTATED_ISOFORM"
        else
          genes[gene] << "MUTATED_ISOFORM"
        end
      end
    end

    res = TSV.setup({}, :key_field => "Ensembl Gene ID", :fields => ["Broken alleles"], :namespace => organism, :type => :single)
    genes.each do |gene,alts|
      broken = if alts.select{|a| a =~ /^H:/ }.any?
                 "BOTH"
               elsif alts.length > 1
                 "BOTH?"
               else
                 "ONE"
               end
      res[gene] = broken
    end
    res
  end
end
