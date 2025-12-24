use rusoto_core::RusotoError;
use ssh2::Session;
use std::io;
use std::io::Read;
use std::net::TcpStream;
use std::path::Path;
use std::{collections::HashMap, fmt::Error};

use rusoto_ec2::{
    CancelSpotInstanceRequestsResult, DescribeInstancesResult, DescribeSpotInstanceRequestsResult,
    Ec2, RequestSpotInstancesError, RequestSpotInstancesResult, Reservation,
    TerminateInstancesResult,
};

pub struct SshConnection {}
struct Burst {}

pub struct MachineSetup {
    instance_type: String,
    ami: String,
    setup: Box<dyn Fn(&mut SshConnection) -> io::Result<()>>,
}

pub struct Machine {
    ssh: Option<SshConnection>,
    instance_type: String,
    ip: String,
    dns: String,
}

pub struct BurstBuilder {
    descriptors: HashMap<String, (MachineSetup, u32)>,
    max_duration: i64,
}

impl MachineSetup {
    pub fn new<F>(instance_type: String, ami: String, setup: F) -> Self
    where
        F: Fn(&mut SshConnection) -> io::Result<()> + 'static,
    {
        MachineSetup {
            instance_type,
            ami,
            setup: Box::new(setup),
        }
    }
}

impl Default for BurstBuilder {
    fn default() -> Self {
        BurstBuilder {
            descriptors: Default::default(),
            max_duration: 60,
        }
    }
}

impl BurstBuilder {
    pub fn add_set(&mut self, name: String, number: u32, setup: MachineSetup)
    // where
    //     F: Fn(&mut SshConnection) -> io::Result<()>,
    {
        self.descriptors.insert(name, (setup, number));
    }

    pub fn set_max_duration(&mut self, hours: u8) {
        self.max_duration = hours as i64 * 60;
    }

    pub async fn run<F>(
        self,
        f: F,
    ) -> Result<Vec<String>, rusoto_core::RusotoError<RequestSpotInstancesError>>
    where
        F: FnOnce(HashMap<String, &mut [Machine]>) -> io::Result<()>,
    {
        let ec2 = rusoto_ec2::Ec2Client::new(rusoto_core::Region::EuNorth1);
        let mut spot_req_ids = Vec::new();
        println!("spotreqqqqqqqqqqq");
        for (name, (setup, number)) in &self.descriptors {
            let mut launch = rusoto_ec2::RequestSpotLaunchSpecification::default();
            launch.image_id = Some(setup.ami.to_string());
            println!("amiiiiiiii");
            launch.instance_type = Some(setup.instance_type.to_string());
            println!("instancetypeeeeeee");
            launch.key_name = Some(String::from("FirstOne"));
            launch.security_group_ids = Some(vec![String::from("sg-0701f648255d191ef")]);
            println!("security grp ids.......");
            // launch.security_groups = Some(vec![String::from("hello")]);

            let mut req = rusoto_ec2::RequestSpotInstancesRequest::default();
            req.instance_count = Some(i64::from(*number));
            req.block_duration_minutes = Some(self.max_duration);
            req.launch_specification = Some(launch);

            let res: RequestSpotInstancesResult = ec2.request_spot_instances(req).await?;
            println!("RequestSpotInstancesResult......{}", format!("{:?}",res.clone()));
            spot_req_ids.extend(
                res.spot_instance_requests
                    .unwrap_or_default()
                    .into_iter()
                    .filter_map(|f| f.spot_instance_request_id),
            );
        }

        let mut req = rusoto_ec2::DescribeSpotInstanceRequestsRequest::default();

        req.spot_instance_request_ids = Some(spot_req_ids.clone());
        let instances: Vec<_>;
        loop {
            let res: DescribeSpotInstanceRequestsResult = ec2
                .describe_spot_instance_requests(req.clone())
                .await
                .map_err(|e| RusotoError::ParseError(e.to_string()))?;
            let spot_request = res.spot_instance_requests.unwrap();
            let any_open = spot_request
                .iter()
                .any(|sir| sir.state.clone().unwrap() == "open");
            if !any_open {
                instances = spot_request
                    .into_iter()
                    .filter_map(|sir| sir.instance_id)
                    .collect();
                break;
            }
        }

        let mut cancel = rusoto_ec2::CancelSpotInstanceRequestsRequest::default();
        cancel.spot_instance_request_ids = spot_req_ids.clone();
        let res: CancelSpotInstanceRequestsResult = ec2
            .cancel_spot_instance_requests(cancel)
            .await
            .map_err(|e| RusotoError::ParseError(e.to_string()))?;
        let cancel_Res = res.cancelled_spot_instance_requests.unwrap();

        let mut any_not_ready = true;
        let mut machines = Vec::new();
        let mut req = rusoto_ec2::DescribeInstancesRequest::default();
        req.instance_ids = Some(instances.clone());
        while any_not_ready {
            any_not_ready = false;
            machines.clear();
            let res: DescribeInstancesResult = ec2
                .describe_instances(req.clone())
                .await
                .map_err(|e| RusotoError::ParseError(e.to_string()))?;
            for reservation in res.reservations.unwrap() {
                for instance in reservation.instances.unwrap() {
                    match instance {
                        rusoto_ec2::Instance {
                            instance_type: Some(instance_type),
                            private_ip_address: Some(ip),
                            public_dns_name: Some(dns),
                            ..
                        } => {
                            let machine = Machine {
                                ssh: None,
                                instance_type,
                                ip,
                                dns,
                            };
                            machines.push(machine);
                        }
                        _ => {
                            any_not_ready = true;
                        }
                    }
                }
            }
            if any_not_ready {
                // sleep a bit before retrying (use tokio::time::sleep since run() is async)
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }

        for machine in &machines {
            let addr = &format!("{}:22", machine.dns);
            eprintln!("Trying to resolve/connect to: {}", addr);
            // if let Ok(addrs) = addr.as_str() {
            //     for a in addrs {
            //         eprintln!("Resolved: {}", a);
            //     }
            // }
            let tcp = TcpStream::connect(&format!("{}:22", machine.dns)).unwrap();
            let privatekeypath = Path::new(r"D:\Downloads\FirstOne (1).pem");
            let mut sess = Session::new().unwrap();
            sess.set_tcp_stream(tcp);
            sess.handshake().unwrap();
            sess.userauth_pubkey_file("ec2-user", None, privatekeypath, None)
                .map_err(|e| RusotoError::ParseError(format!("SSH auth failed: {}", e)))?;
            // sess.userauth_agent("ec2-user").unwrap();

            if !sess.authenticated() {
                return Err(RusotoError::ParseError(format!(
                    "SSH auth failed for............"
                )));
            }

            let mut channel = sess.channel_session().unwrap();
            channel.exec("ip addr show up").unwrap();
            let mut s = String::new();
            channel.read_to_string(&mut s).unwrap();
            println!("{}", s);
            channel.wait_close().unwrap();
            println!("{}", channel.exit_status().unwrap());
        }
        let mut terminate_instances_request = rusoto_ec2::TerminateInstancesRequest::default();
        terminate_instances_request.instance_ids = req.instance_ids.unwrap();
        let res: TerminateInstancesResult = ec2
            .terminate_instances(terminate_instances_request)
            .await
            .map_err(|e| RusotoError::ParseError(e.to_string()))?;
        Ok(spot_req_ids)
    }
}
