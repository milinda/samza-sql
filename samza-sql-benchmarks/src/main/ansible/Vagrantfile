# -*- mode: ruby -*-
# vi: set ft=ruby :

$num_instances = 9
$instance_name_prefix = "samzasql-"
$vm_gui = false
$vm_memory = 2048
$vm_cpus = 2


Vagrant.configure(2) do |config|
  config.vm.box = "ubuntu/trusty64"

  (1..$num_instances).each do |i|
    config.vm.define vm_name = "%s-%02d" % [$instance_name_prefix, i] do |config|
      config.vm.hostname = vm_name

      config.vm.provider :virtualbox do |vb|
        vb.gui = $vm_gui
        vb.memory = $vm_memory
        vb.cpus = $vm_cpus
      end

      ip = "172.17.8.#{i+100}"
      config.vm.network :private_network, ip: ip
      config.vm.network :forwarded_port, guest: 2181, host: i+10181
      config.vm.network :forwarded_port, guest: 9091, host: i+11091
      config.vm.network :forwarded_port, guest: 8088, host: i+10088
      config.vm.network :forwarded_port, guest: 8032, host: i+10032
      config.vm.network :forwarded_port, guest: 50060, host: i+50060     

      config.vm.provision "ansible" do |ansible|
        ansible.playbook = "ansible_kafka.yml"
      end
    end
  end
end
