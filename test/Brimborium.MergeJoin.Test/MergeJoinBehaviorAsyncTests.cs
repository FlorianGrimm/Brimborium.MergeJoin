namespace Brimborium.MergeJoin.Test;

public class MergeJoinBehaviorAsyncTests {
    [Test]
    public async Task SourceAndTargetEqualTest() {
        var act = await ExecuteMergeJoinAsync(
            new List<SourceData>() { new SourceData(1, "A"), new SourceData(2, "B"), new SourceData(3, "C") },
            new List<TargetData>() { new TargetData(1, "A"), new TargetData(2, "B"), new TargetData(3, "C") });

        await Assert.That(act.Inserts).IsEquivalentTo(new List<SourceData>());
        await Assert.That(act.Updates).IsEquivalentTo(new List<PairData>() {
                new PairData(new SourceData(1, "A"), new TargetData(1, "A")),
                new PairData(new SourceData(2, "B"), new TargetData(2, "B")),
                new PairData(new SourceData(3, "C"), new TargetData(3, "C"))
                });
        await Assert.That(act.Deletes).IsEquivalentTo(new List<TargetData>());
    }

    [Test]
    public async Task SourceHasMoreItemsTest() {
        var act = await ExecuteMergeJoinAsync(
            new List<SourceData>() { new SourceData(1, "A"), new SourceData(2, "B"), new SourceData(3, "C"), new SourceData(4, "D") },
            new List<TargetData>() { new TargetData(1, "A"), new TargetData(3, "C") });

        await Assert.That(act.Inserts).IsEquivalentTo(new List<SourceData>() {
            new SourceData(2, "B"), new SourceData(4, "D")
        });
        await Assert.That(act.Updates).IsEquivalentTo(new List<PairData>() {
            new PairData(new SourceData(1, "A"), new TargetData(1, "A")),
            new PairData(new SourceData(3, "C"), new TargetData(3, "C"))
        });
        await Assert.That(act.Deletes).IsEquivalentTo(new List<TargetData>());
    }

    [Test]
    public async Task TargetHasMoreItemsTest() {
        var act = await ExecuteMergeJoinAsync(
            new List<SourceData>() { new SourceData(1, "A"), new SourceData(3, "C") },
            new List<TargetData>() { new TargetData(1, "A"), new TargetData(2, "B"), new TargetData(3, "C"), new TargetData(4, "D") });

        await Assert.That(act.Inserts).IsEquivalentTo(new List<SourceData>());
        await Assert.That(act.Updates).IsEquivalentTo(new List<PairData>() {
            new PairData(new SourceData(1, "A"), new TargetData(1, "A")),
            new PairData(new SourceData(3, "C"), new TargetData(3, "C"))
        });
        await Assert.That(act.Deletes).IsEquivalentTo(new List<TargetData>() {
            new TargetData(2, "B"), new TargetData(4, "D")
        });
    }

    [Test]
    public async Task EmptySourceTest() {
        var act = await ExecuteMergeJoinAsync(
            new List<SourceData>(),
            new List<TargetData>() { new TargetData(1, "A"), new TargetData(2, "B"), new TargetData(3, "C") });

        await Assert.That(act.Inserts).IsEquivalentTo(new List<SourceData>());
        await Assert.That(act.Updates).IsEquivalentTo(new List<PairData>());
        await Assert.That(act.Deletes).IsEquivalentTo(new List<TargetData>() {
            new TargetData(1, "A"), new TargetData(2, "B"), new TargetData(3, "C")
        });
    }

    [Test]
    public async Task EmptyTargetTest() {
        var act = await ExecuteMergeJoinAsync(
            new List<SourceData>() { new SourceData(1, "A"), new SourceData(2, "B"), new SourceData(3, "C") },
            new List<TargetData>());

        await Assert.That(act.Inserts).IsEquivalentTo(new List<SourceData>() {
            new SourceData(1, "A"), new SourceData(2, "B"), new SourceData(3, "C")
        });
        await Assert.That(act.Updates).IsEquivalentTo(new List<PairData>());
        await Assert.That(act.Deletes).IsEquivalentTo(new List<TargetData>());
    }

    [Test]
    public async Task InterleavedItemsTest() {
        var act = await ExecuteMergeJoinAsync(
            new List<SourceData>() { new SourceData(1, "A"), new SourceData(3, "C"), new SourceData(5, "E") },
            new List<TargetData>() { new TargetData(2, "B"), new TargetData(4, "D"), new TargetData(6, "F") });

        await Assert.That(act.Inserts).IsEquivalentTo(new List<SourceData>() {
            new SourceData(1, "A"), new SourceData(3, "C"), new SourceData(5, "E")
        });
        await Assert.That(act.Updates).IsEquivalentTo(new List<PairData>());
        await Assert.That(act.Deletes).IsEquivalentTo(new List<TargetData>() {
            new TargetData(2, "B"), new TargetData(4, "D"), new TargetData(6, "F")
        });
    }

    [Test]
    public async Task UpdateWithDifferentValuesTest() {
        var act = await ExecuteMergeJoinAsync(
            new List<SourceData>() { new SourceData(1, "A"), new SourceData(2, "Updated B"), new SourceData(3, "C") },
            new List<TargetData>() { new TargetData(1, "A"), new TargetData(2, "B"), new TargetData(3, "C") });

        await Assert.That(act.Inserts).IsEquivalentTo(new List<SourceData>());
        await Assert.That(act.Updates).IsEquivalentTo(new List<PairData>() {
            new PairData(new SourceData(1, "A"), new TargetData(1, "A")),
            new PairData(new SourceData(2, "Updated B"), new TargetData(2, "B")),
            new PairData(new SourceData(3, "C"), new TargetData(3, "C"))
        });
        await Assert.That(act.Deletes).IsEquivalentTo(new List<TargetData>());
    }

    private async Task<MergeJoinBehaviorAsyncForTest> ExecuteMergeJoinAsync(
        IEnumerable<SourceData> listSource,
        IEnumerable<TargetData> listTarget
    ) {
        var listSourceOrdered = listSource.OrderBy(x => x.Id);
        var listTargetOrdered = listTarget.OrderBy(x => x.Id);

        var result = new MergeJoinBehaviorAsyncForTest();
        await result.ExecuteAsync(listSourceOrdered, listTargetOrdered);
        return result;
    }

    [Test]
    public async Task SoureIsNotSortedFailedTest() {
        await Assert.ThrowsAsync<ArgumentException>(async () => {
            var listSource = new List<SourceData>() { new SourceData(2, "Updated B"), new SourceData(1, "A"), new SourceData(3, "C") };
            var listTarget = new List<TargetData>() { new TargetData(1, "A"), new TargetData(2, "B"), new TargetData(3, "C") };
            var result = new MergeJoinBehaviorAsyncForTest();
            await result.ExecuteAsync(listSource, listTarget);
        });
    }

    [Test]
    public async Task TargetIsNotSortedFailedTest() {
        await Assert.ThrowsAsync<ArgumentException>(async () => {
            var listSource = new List<SourceData>() { new SourceData(1, "A"), new SourceData(2, "Updated B"), new SourceData(3, "C") };
            var listTarget = new List<TargetData>() { new TargetData(2, "B"), new TargetData(1, "A"), new TargetData(3, "C") };
            var result = new MergeJoinBehaviorAsyncForTest();
            await result.ExecuteAsync(listSource, listTarget);
        });
    }
}

public class MergeJoinBehaviorAsyncForTest : MergeJoinBehaviorAsync<SourceData, TargetData> {
    private Comparer<int> _Comparer;

    public MergeJoinBehaviorAsyncForTest() {
        this._Comparer = Comparer<int>.Default;
    }

    protected override int KeyComparer(SourceData source, TargetData target) {
        return this._Comparer.Compare(source.Id, target.Id);
    }
    protected override int SourceCompare(SourceData before, SourceData after) {
        return this._Comparer.Compare(before.Id, after.Id);
    }

    protected override int TargetCompare(TargetData before, TargetData after) {
        return this._Comparer.Compare(before.Id, after.Id);
    }

    public List<SourceData> Inserts { get; } = new();

    protected override async Task InsertAction(SourceData source) {
        this.Inserts.Add(source);
        await Task.CompletedTask;
    }

    public List<PairData> Updates { get; } = new();

    protected override async Task UpdateAction(SourceData source, TargetData target) {
        this.Updates.Add(new PairData(source, target));
        await Task.CompletedTask;
    }

    public List<TargetData> Deletes { get; } = new();

    protected override async Task DeleteAction(TargetData target) {
        this.Deletes.Add(target);
        await Task.CompletedTask;
    }
}