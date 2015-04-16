module Sample

  dep :affected_splicing
  dep :mutated_isoform
  dep :homozygous
  task :affected_alleles => :tsv do
    genes = {}
    Step.wait_for_jobs dependencies
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
  dep :damaged_isoform
  dep :homozygous
  task :broken_alleles => :tsv do
    genes = {}
    homozygous = Set.new step(:homozygous).load

    Step.wait_for_jobs [step(:affected_splicing)]
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

    TSV.traverse step(:damaged_isoform) do |gene, mutations|
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

  dep :affected_alleles
  task :altered_genes => :array do 
    TSV.traverse step(:affected_alleles), :into => :stream do |gene, alleles|
      gene
    end
  end

  dep :broken_alleles
  task :damaged_genes => :array do 
    TSV.traverse step(:broken_alleles), :into => :stream do |gene, alleles|
      gene
    end
  end

  dep :broken_alleles
  task :broken_genes => :array do 
    TSV.traverse step(:broken_alleles), :into => :stream do |gene, alleles|
      next unless alleles =~ /BOTH/
      gene 
    end
  end

  dep :broken_alleles
  task :surely_broken_genes => :array do 
    TSV.traverse step(:broken_alleles), :into => :stream do |gene, alleles|
      next unless alleles == "BOTH"
      gene 
    end
  end
end
