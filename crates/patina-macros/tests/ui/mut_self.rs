use patina_macros::service;

#[service]
trait Bad {
    async fn takes_mut(&mut self) -> Result<(), PatinaError>;
}

fn main() {}
