require 'test/unit'
require 'rbbt/workflow'
require 'rbbt/monitor'

Workflow.require_workflow "Sample"
class TestClass < Test::Unit::TestCase
  def test_true
    Log.severity = 4
    Misc.use_lock_id = false
    #ENV["RBBT_NO_PROGRESS"] = "true"
    #ENV["RBBT_NO_STREAM"] = "true"

    Rbbt.dump_memory "/tmp/mem_dump", String
    sample = 'Test'
    dep_content = {}

    $cpus = 0
    job = Sample.job(:annotations, sample)
    job.recursive_clean
    job.run(true)
    job.join
    normal_size = File.size job.path
    assert_equal 2, job.path.read.split("\n").select{|l| l =~ /#/}.length

    job.dependencies.each do |dep|
      content = dep.path.read.split("\n").sort
      dep_content[dep.path] = content
      count = content.select{|l| l =~ /#/}.length
    end
    assert_equal 2, job.path.read.split("\n").select{|l| l =~ /#/}.length

    $cpus = 20
    job = Sample.job(:annotations, sample)
    job.recursive_clean
    job.run(true)
    job.join

    job.dependencies.each do |dep|
      content = dep.path.read.split("\n").sort
      io = dep.instance_variable_get(:@stream_data)
      if io
        io.rewind
        io_text = io.read
        stream_data = io_text.split("\n").sort
      else
        stream_data = []
      end

      count = content.select{|l| l =~ /#/}.length
      prev = dep_content[dep.path] || []
      prev_text = prev * "\n" + "\n"
      io_text ||= ""
      puts "CPUS: " << dep.path
      puts "  database: " << dep.inputs[0]
      puts "  file: " << File.size(dep.path).to_s
      puts "  prev: " << prev_text.length.to_s
      puts "  stream: " << io_text.length.to_s
      puts "  file == normal: " << (content == prev).to_s 
      puts "  file == stream: " << (content == stream_data).to_s
      puts "  stream == normal: " << (prev == stream_data).to_s
      puts "  header: " << count.to_s
      assert_equal content, prev
    end

    assert_equal 2, job.path.read.split("\n").select{|l| l =~ /#/}.length

    assert_equal normal_size, File.size(job.path)
  end
end

