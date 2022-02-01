#r "nuget: Akka.FSharp" 
#r "nuget: Akka.TestKit" 

open System
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Diagnostics
open System.Collections.Generic
open System.Security.Cryptography
open System.Text


type NodeMessageTypes =
    | AssignSuccessor of int*int
    | Query of int*int
    | Destination of int
    | FingerTable of int


let mutable numNodes = int (string (fsi.CommandLineArgs.GetValue 1))
let mutable numRequests = int (string (fsi.CommandLineArgs.GetValue 2))
let mutable ttl = 0
let mutable ttlhops = 0
let mutable avgHops:float = 0.0
let mutable count = 0
let mutable nodeKeyRef = new Dictionary<int, IActorRef>()

let mutable listNodeHashVal = []
// let mutable (listNodes: IActorRef list) = []

// let mutable requests = [||]

let mutable m = int(Math.Log(float(numNodes), 2.))

let mutable numOfNodes = int(2.0**float(m)) 

let asciiNodes =
    Seq.map int

let sumAsciiValues p=
    Seq.sum p

let asciiModten t=
    string(t % numOfNodes)


// Hash function for Nodes
let hashFunction m=
    let floatM=float(m)
    let twoPowerM=int(2.0**floatM)
    for x in [1..twoPowerM] do
        let mutable result=
            "N"+string(x)
            |>System.Text.Encoding.ASCII.GetBytes 
            |> (new SHA1Managed()).ComputeHash 
            |> Array.map (fun (y : byte) -> System.String.Format("{0:X2}", y)) 
            |> String.concat System.String.Empty
            |> asciiNodes
            |> sumAsciiValues
            |> asciiModten
        listNodeHashVal<-List.append listNodeHashVal [int(result)]  

    listNodeHashVal<-List.distinct listNodeHashVal
    listNodeHashVal<-List.sort listNodeHashVal
    ttl <- numRequests * listNodeHashVal.Length

                  


let PeerSystem = ActorSystem.Create("System")

// Function to create finger table for all the elements.

let PeerActor(mailbox: Actor<_>) =
    let mutable id = 0
    let mutable successor = 0
    let mutable fingertable = []


    let rec findSuccessor n:int =
        if List.contains n listNodeHashVal then 
            n  
        elif n > listNodeHashVal.[(listNodeHashVal.Length)-1] then
            (listNodeHashVal.[0])                   
        else findSuccessor(n+1)
    let createFingerTable i =
        for k in [1..m] do
            let mutable val1=(float(i)+2.0**(float(k)-1.0))%2.0**float(m)
            fingertable <- List.append fingertable [findSuccessor(int(val1))]

    
    
    let rec loop()= actor{
        let! message = mailbox.Receive();

        match message with 
        | AssignSuccessor (index,lst) ->
            id <- listNodeHashVal.[index]
            if lst = 0 then
                successor <- listNodeHashVal.[index+1]
            else
                successor <- listNodeHashVal.[0]
        
        | Query (keyId,hops) ->
            if keyId = id then
                mailbox.Self <! Destination (hops)
            else 

                //Logic for hoping from node to node refering the finger table 
                if keyId > id then
                    if keyId > listNodeHashVal.[listNodeHashVal.Length-1] then
                        nodeKeyRef.[listNodeHashVal.[0]] <! Destination(hops+1)


                    let mutable n = 0
                    let mutable prev = -1
                    while n<m do
                        if keyId = fingertable.[n] then

                            nodeKeyRef.[fingertable.[n]] <! Query(keyId,hops+1)
                            n <- m
                        elif keyId > fingertable.[n] then 

                            if fingertable.[n] < prev then 

                                nodeKeyRef.[prev] <! Query(keyId,hops+1)
                                n <- m
                            else 
                                if n = m-1 then
                                    nodeKeyRef.[fingertable.[n]] <! Query(keyId,hops+1)   
                                prev <- fingertable.[n]
                                n <- n+1
                                
                        else 
                            if prev = -1 then
                                nodeKeyRef.[fingertable.[n]] <! Destination(hops+1)  
                            else  
                                nodeKeyRef.[prev] <! Query(keyId,hops+1)
                            n <- m

                else
                    if keyId <= listNodeHashVal.[0] then
                        
                        nodeKeyRef.[listNodeHashVal.[0]] <! Destination(hops+1)
                    else 
                        nodeKeyRef.[listNodeHashVal.[0]] <! Query(keyId,hops+1)

                
                     


        | FingerTable j -> 
            createFingerTable j     
            
        | Destination (hops) ->
            
            count <- count+1
            ttlhops <- ttlhops + hops 
            if count >= ttl-3 then

                avgHops <- float(ttlhops)/float(count)
                printfn "Average number of hops = %f" avgHops
                Environment.Exit(0)
            
            
        | _-> ()
        
        
        return! loop()
    }            
    loop()


hashFunction m


for x in [0..listNodeHashVal.Length-1] do
    let key: string = "N" + string(x)
    let mutable lst = 0 
    let actorRef = spawn PeerSystem (key) PeerActor

    nodeKeyRef.Add(listNodeHashVal.[x], actorRef)
    // listNodes <- List.append listNodes [actorRef]
    if x = listNodeHashVal.Length-1 then
        lst <- 1
    nodeKeyRef.[listNodeHashVal.[x]] <! AssignSuccessor (x,lst)


for y in [0..listNodeHashVal.Length-1] do
    nodeKeyRef.[listNodeHashVal.[y]] <! FingerTable listNodeHashVal.[y]


for nodes in [0..listNodeHashVal.Length-1] do
    for reqs in [1..numRequests] do
        let rnd1 = Random().Next(0, numOfNodes-1)
        nodeKeyRef.[listNodeHashVal.[nodes]] <! Query (rnd1,0)

let timer = new Timers.Timer()
let event = Async.AwaitEvent (timer.Elapsed) |> Async.Ignore

timer.Start()
let mutable q = 0
while q<100 do 
    Async.RunSynchronously event
    for o in [0..listNodeHashVal.Length-1] do
        nodeKeyRef.[listNodeHashVal.[o]] <! FingerTable listNodeHashVal.[o]
    q<-q+1

    


Console.ReadLine() |> ignore
