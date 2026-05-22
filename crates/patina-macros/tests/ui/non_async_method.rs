use patina_macros::service;

#[service]
trait Bad {
    fn not_async(&self) -> Result<(), PatinaError>;
}

fn main() {}
