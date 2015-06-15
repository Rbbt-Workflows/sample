module Sample
  dep :organism
  input :file, :file, "Input file"
  input :vcf, :boolean, "Input file is a VCF", false
  task :cnvs => :array do |file, vcf|
    stream = if file
               if vcf
                 job = Sequence.job(:cnvs, sample, :vcf_file => file)
                 TSV.get_stream job.run(true)
               else
                 TSV.get_stream file
               end
             else
               TSV.get_stream Sample.cnvs(sample)
             end
    Misc.sensiblewrite(path, CMD.cmd('grep ":" | sed "s/^M:/MT:/" | env LC_ALL=C  sort -u -k1,1 -k2,2 -k3,3 -g -t:', :in => stream, :pipe => true, :no_fail => true))
    nil
  end

  dep :cnvs
  dep :organism
  dep Sequence, :genes_at_ranges, :ranges => :cnvs, :organism => :organism
  task :gene_cnv_status => :tsv do 
    cnv_genes = step(:genes_at_ranges)
    organism = step(:organism).load
    gene_status = TSV.setup({}, :key_field => "Ensembl Gene ID", :fields => ["CNV status"], :namespace => organism, :type => :flat)
    TSV.traverse cnv_genes, :bar => "Processing CNV genes" do |cnv,genes|
      chr,start,eend,somatic,germline = cnv.split(":")

      somatic_cn, somatic_mcn = somatic.split("-").collect{|v| v.to_f}
      germline_cn, germline_mcn = germline ? germline.split("-").collect{|v| v.to_f} : [2,1]

      status = case 
               when somatic_cn > 2
                 "Gain"
               when somatic_cn < 2
                 "Loss"
               else
                 next
               end

      genes.each do |gene|
        gene_status[gene] ||= []
        gene_status[gene] << [status,cnv] * " "
      end
    end
    gene_status
  end

end

