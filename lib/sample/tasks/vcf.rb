module Sample


  dep Sequence, :affected_genes
  input :vcf, :boolean, "Input is VCF", true
  input :info, :boolean, "Keep the preamble of the VCF file", true
  input :format, :boolean, "Keep the preamble of the VCF file", true
  input :preamble, :boolean, "Keep the preamble of the VCF file", true
  task :add_vcf_column => :tsv do |vcf|
    exp_vcf = step(:expanded_vcf)

    new = TSV.traverse step(:affected_genes).join, :type => :array, :into => :stream do |line|
      str = if line =~ /^#/
              line 
            else
              mutation, *values = line.split "\t"
              values.reject!{|v| v.nil? or v.empty?} unless values
              if values and values.any?
                [mutation, values * "|"] * "\t"
              else
                mutation + "\t"
              end
            end
      str
    end

    pasted = TSV.paste_streams([exp_vcf, new], :sort => true, :preamble => true)
    Sequence::VCF.save_stream(pasted)
  end
end

class IOAF
  def close(*args)
    puts( caller * "\n")
    begin
      super(*args)
    rescue Exception
    end
  end 
end
