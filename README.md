# Brimborium.MergeJoin

A merge-join algorithm for efficiently comparing and synchronizing two collections.

## Overview

Brimborium.MergeJoin provides a generic implementation of the merge-join algorithm that allows you to:

- Compare two ordered collections
- Identify items that need to be inserted, updated, or deleted
- Apply custom actions for each operation type
- Handle various data synchronization scenarios

## Key Features

- Generic implementation works with any data types
- Efficient O(n+m) time complexity
- Customizable comparison logic
- Flexible action handlers for inserts, updates, and deletes
- Minimal dependencies

## Usage

```csharp
// Define your source and target types
public record SourceItem(int Id, string Name);
public record TargetItem(int Id, string Name);

// Create a custom MergeJoinBehavior implementation
public class MyMergeJoin : MergeJoinBehavior<SourceItem, TargetItem> 
{
    protected override int KeyComparer(SourceItem source, TargetItem target) 
    {
        return source.Id.CompareTo(target.Id);
    }
    
    protected override void InsertAction(SourceItem source) 
    {
        Console.WriteLine($"Insert: {source.Id} - {source.Name}");
    }
    
    protected override void UpdateAction(SourceItem source, TargetItem target) 
    {
        Console.WriteLine($"Update: {target.Id} from '{target.Name}' to '{source.Name}'");
    }
    
    protected override void DeleteAction(TargetItem target) 
    {
        Console.WriteLine($"Delete: {target.Id} - {target.Name}");
    }
}

// Execute the merge-join
var sourceItems = GetSourceItems().OrderBy(x => x.Id);
var targetItems = GetTargetItems().OrderBy(x => x.Id);

var mergeJoin = new MyMergeJoin();
mergeJoin.Execute(sourceItems, targetItems);
```

## Requirements

- .NET 9.0 or higher

## License

MIT License
