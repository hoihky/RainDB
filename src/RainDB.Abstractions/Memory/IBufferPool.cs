namespace RainDB.Memory;

/// <summary>
/// Low-latency byte buffers for I/O and vector materialization (DIP: mock in tests).
/// Prefer lengths under the LOH threshold (~85KB on 64-bit CLR) when renting from <see cref="ArrayPool{T}"/>.
/// </summary>
public interface IBufferPool
{
    byte[] Rent(int minimumLength);

    void Return(byte[] buffer, bool clearArray = false);
}
