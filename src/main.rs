fn main() {
    let x = 5; // immutable
    println!("x is: {}", x);

    let mut y = 10; // mutable
    println!("y is: {}", y);
    y = 15;
    println!("y is now: {}", y);
}
